// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_pipeline.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <new>
#include <utility>

namespace mino::bridge {
namespace {

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Corruption(const char* message) {
    return Status::Error(StatusCode::kCorruption, message);
}
Status Exhausted(const char* message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}
Status AllocationFailure() {
    return Exhausted("bridge pipeline allocation failed");
}

bool IsWouldBlock(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout;
}

bool IsRetryableIngress(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout ||
           status.code() == StatusCode::kResourceExhausted ||
           status.code() == StatusCode::kUnavailable ||
           status.code() == StatusCode::kDegraded;
}

SourceIdentity SourceFrom(const WireFrameHeader& header) noexcept {
    return SourceIdentity{
        .node_id = header.source_node_id,
        .publisher_id = header.source_publisher_id,
        .publisher_epoch = header.source_publisher_epoch,
    };
}

bool ValidSource(const SourceIdentity& source) noexcept {
    return source.node_id != 0 && source.publisher_id != 0 &&
           source.publisher_epoch != 0;
}

bool ValidLane(uint16_t lane_index, uint16_t lane_count) noexcept {
    return lane_count != 0 && lane_count <= kMaxBridgeLaneCount &&
           lane_index < lane_count;
}

bool SourceBelongsToLane(const SourceIdentity& source, uint16_t lane_index,
                         uint16_t lane_count) noexcept {
    return BridgeLaneFor(source, lane_count) == lane_index;
}

size_t RetainedFrameBytes(const WireFrame& frame) noexcept {
    size_t bytes = kWireBaseHeaderLength + frame.payload.size();
    if (frame.header.perf_trace.has_value()) {
        bytes += kWirePerfTraceContextLength;
    }
    if (HasFrameFlag(frame.header.flags,
                     FrameFlag::kPayloadCrcPresent)) {
        bytes += kWirePayloadCrcLength;
    }
    return bytes;
}

WireFrame ControlFrame(FrameType type, std::vector<std::byte> payload) {
    WireFrame frame;
    frame.header.frame_type = type;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame) |
                         FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.payload = std::move(payload);
    return frame;
}

}  // namespace

bool BridgePipeline::AttemptKeyLess::operator()(
    const AttemptKey& left, const AttemptKey& right) const noexcept {
    if (left.source.node_id != right.source.node_id) {
        return left.source.node_id < right.source.node_id;
    }
    if (left.source.publisher_id != right.source.publisher_id) {
        return left.source.publisher_id < right.source.publisher_id;
    }
    if (left.source.publisher_epoch != right.source.publisher_epoch) {
        return left.source.publisher_epoch < right.source.publisher_epoch;
    }
    if (left.sequence != right.sequence) {
        return left.sequence < right.sequence;
    }
    if (left.connection_id != right.connection_id) {
        return left.connection_id < right.connection_id;
    }
    return left.operation_id < right.operation_id;
}

size_t BridgePipeline::SendOperationHash::operator()(
    transport::SendOperation operation) const noexcept {
    size_t value = static_cast<size_t>(operation.connection_id);
    value ^= static_cast<size_t>(operation.id) + 0x9e3779b97f4a7c15ull +
             (value << 6) + (value >> 2);
    return value;
}

size_t BridgePipeline::ReliableKeyHash::operator()(
    const ReliableKey& key) const noexcept {
    size_t value = SourceIdentityHash{}(key.source);
    value ^= static_cast<size_t>(key.sequence) + 0x9e3779b97f4a7c15ull +
             (value << 6) + (value >> 2);
    return value;
}

Result<std::unique_ptr<BridgePipeline>> BridgePipeline::Create(
    BridgePipelineOptions options,
    std::shared_ptr<transport::TransportDriver> driver,
    transport::ConnectionId connection_id, BridgeEgressPort* egress,
    BridgeIngressPort* ingress,
    SchemaNegotiator* schema_negotiator) noexcept {
    try {
        if (options.local_session_epoch == 0 ||
            options.remote_session_epoch == 0 || driver == nullptr ||
            connection_id == transport::kInvalidConnectionId ||
            ingress == nullptr || options.max_control_frames == 0 ||
            options.max_control_bytes < kSessionHelloHeaderWireSize ||
            options.max_pending_inbound_frames == 0 ||
            options.max_pending_inbound_bytes == 0 ||
            !ValidLane(options.lane_index, options.lane_count)) {
            return Invalid("bridge pipeline dependencies or limits are invalid");
        }
        MINO_ASSIGN_OR_RETURN(auto dedup,
                              DedupWindow::Create(options.dedup));
        MINO_ASSIGN_OR_RETURN(auto retransmit,
                              RetransmitWindow::Create(options.retransmit));
        dedup->BeginSession(options.remote_session_epoch, 0);
        retransmit->BeginSession(options.local_session_epoch,
                                 options.remote_session_epoch, 0);
        auto pipeline = std::unique_ptr<BridgePipeline>(new BridgePipeline(
            options, std::move(driver), connection_id, egress, ingress,
            schema_negotiator, std::move(dedup), std::move(retransmit)));
        pipeline->attempt_operations_.reserve(
            options.retransmit.max_entries);
        pipeline->pending_reliable_.reserve(options.retransmit.max_entries);
        pipeline->pending_reliable_index_.reserve(
            options.retransmit.max_entries);
        pipeline->pending_reliable_sources_.reserve(
            options.retransmit.max_entries);
        pipeline->pending_inbound_.reserve(
            options.max_pending_inbound_frames);
        pipeline->staged_ready_.reserve(
            options.max_pending_inbound_frames);
        pipeline->pending_ack_sources_.reserve(options.max_control_frames);
        pipeline->pending_schema_controls_.reserve(
            options.max_control_frames);
        pipeline->local_schema_bindings_.reserve(
            options.retransmit.max_entries);
        MINO_RETURN_IF_ERROR(pipeline->QueueSessionHello());
        return pipeline;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

BridgePipeline::BridgePipeline(
    BridgePipelineOptions options,
    std::shared_ptr<transport::TransportDriver> driver,
    transport::ConnectionId connection_id, BridgeEgressPort* egress,
    BridgeIngressPort* ingress, SchemaNegotiator* schema_negotiator,
    std::unique_ptr<DedupWindow> dedup,
    std::unique_ptr<RetransmitWindow> retransmit) noexcept
    : options_(options),
      driver_(std::move(driver)),
      connection_id_(connection_id),
      egress_(egress),
      ingress_(ingress),
      schema_negotiator_(schema_negotiator),
      dedup_(std::move(dedup)),
      retransmit_(std::move(retransmit)) {}

Status BridgePipeline::reliability_status() const {
    return reliability_degraded()
               ? Status::Error(StatusCode::kDegraded,
                               "Bridge receiver dedup state was lost")
               : Status::Ok();
}

Status BridgePipeline::QueueControl(const WireFrame& frame) noexcept {
    try {
        auto encoded = WireFrameCodec::Encode(frame, options_.wire_limits);
        if (!encoded.ok()) return encoded.status();
        if (control_queue_.size() >= options_.max_control_frames ||
            encoded->size() > options_.max_control_bytes - control_bytes_) {
            return Exhausted("bridge control queue is full");
        }
        control_bytes_ += encoded->size();
        control_queue_.push_back(std::move(*encoded));
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::QueueNegotiatedControls(
    const std::vector<WireFrame>& controls) noexcept {
    try {
        for (const WireFrame& control : controls) {
            const bool duplicate = std::any_of(
                pending_schema_controls_.begin(),
                pending_schema_controls_.end(),
                [&control](const WireFrame& pending) {
                    return pending == control;
                });
            if (duplicate) continue;
            if (pending_schema_controls_.size() >=
                options_.max_control_frames) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "bridge pending schema control queue is full");
            }
            pending_schema_controls_.push_back(control);
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::AdmitNegotiatedControls() noexcept {
    while (!pending_schema_controls_.empty()) {
        const WireFrame& control = pending_schema_controls_.front();
        const Status queued = QueueControl(control);
        if (!queued.ok()) {
            return queued.code() == StatusCode::kResourceExhausted
                       ? Status::Ok()
                       : queued;
        }
        if (control.header.frame_type == FrameType::kSchemaRequest) {
            const Status confirmed =
                schema_negotiator_->ConfirmControlQueued(control);
            if (!confirmed.ok()) {
                control_bytes_ -= control_queue_.back().size();
                control_queue_.pop_back();
                if (confirmed.code() == StatusCode::kInvalidArgument) {
                    pending_schema_controls_.erase(
                        pending_schema_controls_.begin());
                    continue;
                }
                return confirmed;
            }
        }
        pending_schema_controls_.erase(
            pending_schema_controls_.begin());
    }
    return Status::Ok();
}

Status BridgePipeline::QueueSessionHello() noexcept {
    try {
        MINO_ASSIGN_OR_RETURN(auto snapshot, dedup_->SnapshotAccepted());
        std::vector<SessionHelloSource> sources;
        sources.reserve(snapshot.size());
        for (const DedupResumeEntry& entry : snapshot) {
            if (!SourceBelongsToLane(entry.source, options_.lane_index,
                                     options_.lane_count)) {
                return Corruption(
                    "bridge dedup snapshot contains a source from another lane");
            }
            sources.push_back(SessionHelloSource{
                .source = entry.source,
                .last_accepted_sequence =
                    entry.highest_contiguous_sequence,
            });
        }
        MINO_ASSIGN_OR_RETURN(
            auto payload,
            ControlPayloadCodec::EncodeSessionHello(SessionHello{
                .sender_session_epoch = options_.local_session_epoch,
                .receiver_session_epoch = options_.remote_session_epoch,
                .dedup_state_lost = options_.local_dedup_state_lost,
                .sources = std::move(sources),
            }));
        return QueueControl(
            ControlFrame(FrameType::kSessionHello, std::move(payload)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::length_error&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::RebindConnection(
    transport::ConnectionId connection_id, uint64_t local_session_epoch,
    uint64_t remote_session_epoch, bool local_dedup_state_lost,
    uint64_t now_ns, security::AuthenticatedPeer authenticated_peer) noexcept {
    try {
        if (connection_id == transport::kInvalidConnectionId ||
            local_session_epoch == 0 || remote_session_epoch == 0 ||
            !ValidLane(options_.lane_index, options_.lane_count)) {
            return Invalid("bridge reconnect identity is incomplete");
        }

        const transport::ConnectionId old_connection_id = connection_id_;
        if (old_connection_id != connection_id) {
            const Status closed = driver_->Close(old_connection_id);
            if (!closed.ok() && closed.code() != StatusCode::kNotFound) {
                return closed;
            }
        }
        connection_id_ = connection_id;
        options_.local_session_epoch = local_session_epoch;
        options_.remote_session_epoch = remote_session_epoch;
        options_.local_dedup_state_lost = local_dedup_state_lost;
        if (authenticated_peer.complete()) {
            options_.authenticated_peer = std::move(authenticated_peer);
        }
        control_queue_.clear();
        control_bytes_ = 0;
        pending_acks_.clear();
        pending_ack_sources_.clear();
        pending_schema_controls_.clear();
        pending_inbound_.clear();
        staged_ready_.clear();
        pending_inbound_bytes_ = 0;
        processing_inbound_wire_bytes_ = 0;
        local_schema_bindings_.clear();
        hello_sent_ = false;
        hello_received_ = false;
        session_ready_ = false;
        peer_dedup_state_lost_ = false;
        resend_pending_ = retransmit_->size() != 0;

        dedup_->BeginSession(remote_session_epoch, now_ns,
                             !local_dedup_state_lost);
        retransmit_->BeginSession(local_session_epoch, remote_session_epoch,
                                  now_ns);
        if (schema_negotiator_ != nullptr) schema_negotiator_->Reset();
        return QueueSessionHello();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

bool BridgePipeline::CanQueueAck(const AckPayload& ack) const noexcept {
    if (pending_acks_.size() < options_.max_control_frames) return true;
    if (ack.disposition != AckDisposition::kAccepted) return false;

    const auto source = pending_ack_sources_.find(ack.source);
    if (source == pending_ack_sources_.end()) return false;
    if (source->second.has_cumulative) {
        const uint64_t queued_highest =
            source->second.cumulative->highest_contiguous_sequence.value_or(0);
        if (ack.observed_sequence <= queued_highest) return true;
        if (ack.highest_contiguous_sequence.has_value() &&
            ack.observed_sequence <= *ack.highest_contiguous_sequence) {
            return true;
        }
    }
    if (source->second.selective.contains(ack.observed_sequence)) return true;
    if (ack.highest_contiguous_sequence.has_value() &&
        ack.observed_sequence <= *ack.highest_contiguous_sequence) {
        const auto covered = source->second.selective.begin();
        return covered != source->second.selective.end() &&
               covered->first <= *ack.highest_contiguous_sequence;
    }
    return false;
}

Status BridgePipeline::QueueAck(const AckPayload& ack) noexcept {
    try {
        if (ack.disposition != AckDisposition::kAccepted) {
            if (pending_acks_.size() >= options_.max_control_frames) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "bridge pending ACK queue is full");
            }
            pending_acks_.push_back(ack);
            return Status::Ok();
        }

        auto source_result = pending_ack_sources_.try_emplace(ack.source);
        auto source = source_result.first;
        const auto erase_empty_source = [&]() {
            if (!source->second.has_cumulative &&
                source->second.selective.empty()) {
                pending_ack_sources_.erase(source);
            }
        };
        const bool cumulative =
            ack.highest_contiguous_sequence.has_value() &&
            ack.observed_sequence <= *ack.highest_contiguous_sequence;
        if (cumulative) {
            const uint64_t highest = *ack.highest_contiguous_sequence;
            if (source->second.has_cumulative) {
                const uint64_t queued_highest = source->second.cumulative
                                                    ->highest_contiguous_sequence
                                                    .value_or(0);
                if (highest <= queued_highest) return Status::Ok();
                *source->second.cumulative = ack;
            } else {
                auto reusable = source->second.selective.begin();
                if (reusable != source->second.selective.end() &&
                    reusable->first <= highest) {
                    source->second.cumulative = reusable->second;
                    source->second.has_cumulative = true;
                    *reusable->second = ack;
                    source->second.selective.erase(reusable);
                } else {
                    if (pending_acks_.size() >=
                        options_.max_control_frames) {
                        erase_empty_source();
                        return Status::Error(
                            StatusCode::kWouldBlock,
                            "bridge pending ACK queue is full");
                    }
                    try {
                        pending_acks_.push_back(ack);
                    } catch (const std::bad_alloc&) {
                        erase_empty_source();
                        return AllocationFailure();
                    }
                    source->second.cumulative = std::prev(pending_acks_.end());
                    source->second.has_cumulative = true;
                }
            }
            auto covered = source->second.selective.begin();
            while (covered != source->second.selective.end() &&
                   covered->first <= highest) {
                pending_acks_.erase(covered->second);
                covered = source->second.selective.erase(covered);
            }
            return Status::Ok();
        }

        if (source->second.has_cumulative) {
            const uint64_t queued_highest =
                source->second.cumulative->highest_contiguous_sequence
                    .value_or(0);
            if (ack.observed_sequence <= queued_highest) return Status::Ok();
        }
        const auto existing =
            source->second.selective.find(ack.observed_sequence);
        if (existing != source->second.selective.end()) {
            *existing->second = ack;
            return Status::Ok();
        }
        if (pending_acks_.size() >= options_.max_control_frames) {
            erase_empty_source();
            return Status::Error(StatusCode::kWouldBlock,
                                 "bridge pending ACK queue is full");
        }
        pending_acks_.push_back(ack);
        const auto pending = std::prev(pending_acks_.end());
        try {
            source->second.selective.emplace(ack.observed_sequence, pending);
        } catch (const std::bad_alloc&) {
            pending_acks_.erase(pending);
            erase_empty_source();
            return AllocationFailure();
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

void BridgePipeline::RemovePendingAck(
    PendingAckQueue::iterator pending) noexcept {
    if (pending->disposition == AckDisposition::kAccepted) {
        const auto source = pending_ack_sources_.find(pending->source);
        if (source != pending_ack_sources_.end()) {
            if (source->second.has_cumulative &&
                source->second.cumulative == pending) {
                source->second.has_cumulative = false;
            }
            const auto selective =
                source->second.selective.find(pending->observed_sequence);
            if (selective != source->second.selective.end() &&
                selective->second == pending) {
                source->second.selective.erase(selective);
            }
            if (!source->second.has_cumulative &&
                source->second.selective.empty()) {
                pending_ack_sources_.erase(source);
            }
        }
    }
    pending_acks_.erase(pending);
}

Status BridgePipeline::FlushAcks(BridgePumpBudget budget,
                                 BridgePumpResult* result) noexcept {
    while (!pending_acks_.empty() &&
           result->outbound_frames < budget.max_outbound_frames) {
        MINO_ASSIGN_OR_RETURN(
            auto payload,
            ControlPayloadCodec::EncodeAck(pending_acks_.front()));
        WireFrame frame = ControlFrame(FrameType::kAck, std::move(payload));
        MINO_ASSIGN_OR_RETURN(
            auto body, WireFrameCodec::Encode(frame, options_.wire_limits));
        if (body.size() > budget.max_bytes - result->bytes) break;
        auto sent = driver_->SendUntracked(transport::UntrackedSendRequest{
            .connection_id = connection_id_,
            .payload = body,
            .traffic_class =
                transport::UntrackedTrafficClass::kProtocolControl,
        });
        if (!sent.ok()) {
            return IsWouldBlock(sent.status()) ? Status::Ok() : sent.status();
        }
        result->bytes += body.size();
        ++result->outbound_frames;
        result->made_progress = true;
        RemovePendingAck(pending_acks_.begin());
    }
    return Status::Ok();
}

Status BridgePipeline::FlushControls(BridgePumpBudget budget,
                                     BridgePumpResult* result) noexcept {
    while (!control_queue_.empty() &&
           result->outbound_frames < budget.max_outbound_frames) {
        const std::vector<std::byte>& body = control_queue_.front();
        if (body.size() > budget.max_bytes - result->bytes) break;
        auto sent = driver_->SendUntracked(transport::UntrackedSendRequest{
            .connection_id = connection_id_,
            .payload = body,
            .traffic_class =
                transport::UntrackedTrafficClass::kProtocolControl,
        });
        if (!sent.ok()) {
            if (IsWouldBlock(sent.status())) return Status::Ok();
            return sent.status();
        }
        control_bytes_ -= body.size();
        result->bytes += body.size();
        ++result->outbound_frames;
        result->made_progress = true;
        control_queue_.pop_front();
        hello_sent_ = true;
    }
    session_ready_ = hello_sent_ && hello_received_;
    return Status::Ok();
}

Status BridgePipeline::DrainCompletions(const BridgePumpBudget& budget,
                                        BridgePumpResult* result) noexcept {
    try {
        if (budget.max_completions == 0) return Status::Ok();
        std::vector<transport::ConnectionId> connection_ids;
        connection_ids.reserve(attempts_.size() + 1);
        connection_ids.push_back(connection_id_);
        for (const auto& [key, attempt] : attempts_) {
            (void)key;
            if (std::find(connection_ids.begin(), connection_ids.end(),
                          attempt.operation.connection_id) ==
                connection_ids.end()) {
                connection_ids.push_back(attempt.operation.connection_id);
            }
        }

        Status current_failure = Status::Ok();
        uint32_t remaining = std::min<uint32_t>(
            budget.max_completions,
            transport::kMaxCompletionBatchOperations);
        for (transport::ConnectionId id : connection_ids) {
            if (remaining == 0) break;
            auto completed = driver_->PollCompletions(
                transport::CompletionPollRequest{
                    .max_completions = remaining,
                    .timeout_ms = 0,
                    .connection_id = id,
                });
            if (!completed.ok()) {
                if (IsWouldBlock(completed.status())) continue;
                return completed.status();
            }
            for (const transport::DeliveryCompletion& completion :
                 completed->completions) {
                const auto indexed =
                    attempt_operations_.find(completion.operation);
                if (indexed != attempt_operations_.end()) {
                    const auto attempt = attempts_.find(indexed->second);
                    if (attempt != attempts_.end()) {
                        if (!completion.status.ok() &&
                            completion.operation.connection_id ==
                                connection_id_) {
                            current_failure = completion.status;
                            resend_pending_ = true;
                            session_ready_ = false;
                        }
                        RemoveAttempt(attempt);
                    } else {
                        attempt_operations_.erase(indexed);
                    }
                }
            }
            const uint32_t count =
                static_cast<uint32_t>(completed->completions.size());
            remaining -= count;
            result->completions += count;
            result->made_progress = result->made_progress || count != 0;
        }
        return current_failure;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::AuthorizeInboundData(
    const WireFrameHeader& header) const noexcept {
    if (header.frame_type != FrameType::kData) return Status::Ok();
    if (!options_.authenticated_peer.complete() &&
        options_.topic_authorizer == nullptr) {
        return Status::Ok();
    }
    if (!options_.authenticated_peer.complete() ||
        options_.topic_authorizer == nullptr) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "bridge inbound authorization is incomplete");
    }
    if (header.source_node_id != options_.authenticated_peer.node_id.value) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "bridge data source does not match authenticated peer");
    }
    if (header.topic_id == 0) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "bridge data topic is zero");
    }
    return options_.topic_authorizer->AuthorizeInbound(
        options_.authenticated_peer, TopicId{header.topic_id});
}

Status BridgePipeline::QueuePendingInbound(WireFrame frame,
                                           size_t wire_bytes,
                                           bool schema_resolved) noexcept {
    try {
        const size_t charge =
            wire_bytes != 0 ? wire_bytes : RetainedFrameBytes(frame);
        if (pending_inbound_.size() >=
                options_.max_pending_inbound_frames ||
            charge > options_.max_pending_inbound_bytes -
                         pending_inbound_bytes_) {
            return Exhausted("bridge pending inbound queue is full");
        }
        pending_inbound_.push_back(PendingInbound{
            .frame = std::move(frame),
            .wire_bytes = charge,
            .schema_resolved = schema_resolved,
        });
        pending_inbound_bytes_ += charge;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::StageReadyFrames(
    std::vector<WireFrame>* frames) noexcept {
    try {
        size_t staged_bytes = 0;
        for (const WireFrame& frame : *frames) {
            const size_t charge = RetainedFrameBytes(frame);
            if (charge > options_.max_pending_inbound_bytes - staged_bytes) {
                return Exhausted("schema-ready batch exceeds inbound byte bound");
            }
            staged_bytes += charge;
        }
        const size_t retained_frames = pending_inbound_.empty()
                                           ? 0
                                           : pending_inbound_.size() - 1;
        const size_t retained_bytes =
            pending_inbound_bytes_ - processing_inbound_wire_bytes_;
        if (frames->size() > options_.max_pending_inbound_frames -
                                 retained_frames ||
            staged_bytes > options_.max_pending_inbound_bytes -
                               retained_bytes) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "schema-ready batch exceeds inbound capacity");
        }
        for (WireFrame& frame : *frames) {
            const size_t charge = RetainedFrameBytes(frame);
            staged_ready_.push_back(PendingInbound{
                .frame = std::move(frame),
                .wire_bytes = charge,
                .schema_resolved = true,
            });
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::ProcessPendingInbound(
    const BridgePumpBudget& budget, BridgePumpResult* result) noexcept {
    size_t processed = 0;
    while (!pending_inbound_.empty() &&
           processed < budget.max_inbound_frames) {
        staged_ready_.clear();
        processing_inbound_wire_bytes_ =
            pending_inbound_.front().wire_bytes;
        const Status handled = pending_inbound_.front().schema_resolved
                                   ? HandleData(
                                         pending_inbound_.front().frame,
                                         budget.now_ns)
                                   : HandleFrame(
                                         pending_inbound_.front().frame,
                                         budget.now_ns, result);
        if (!handled.ok()) {
            staged_ready_.clear();
            processing_inbound_wire_bytes_ = 0;
            return IsRetryableIngress(handled) ? Status::Ok() : handled;
        }
        pending_inbound_bytes_ -= pending_inbound_.front().wire_bytes;
        pending_inbound_.erase(pending_inbound_.begin());
        size_t staged_bytes = 0;
        for (const PendingInbound& ready : staged_ready_) {
            staged_bytes += ready.wire_bytes;
        }
        pending_inbound_.insert(
            pending_inbound_.begin(),
            std::make_move_iterator(staged_ready_.begin()),
            std::make_move_iterator(staged_ready_.end()));
        pending_inbound_bytes_ += staged_bytes;
        staged_ready_.clear();
        processing_inbound_wire_bytes_ = 0;
        ++processed;
        result->made_progress = true;
    }
    return Status::Ok();
}

Status BridgePipeline::DrainInbound(const BridgePumpBudget& budget,
                                    BridgePumpResult* result) noexcept {
    MINO_RETURN_IF_ERROR(ProcessPendingInbound(budget, result));
    if (!pending_inbound_.empty() || budget.max_inbound_frames == 0 ||
        result->bytes >= budget.max_bytes) {
        return Status::Ok();
    }

    const size_t frame_capacity = options_.max_pending_inbound_frames -
                                  pending_inbound_.size();
    const size_t byte_capacity = options_.max_pending_inbound_bytes -
                                 pending_inbound_bytes_;
    if (frame_capacity == 0 || byte_capacity == 0) return Status::Ok();
    const size_t remaining = std::min(budget.max_bytes - result->bytes,
                                      byte_capacity);
    auto received = driver_->Poll(transport::ReceiveRequest{
        .max_messages = static_cast<uint32_t>(std::min<size_t>(
            {budget.max_inbound_frames,
             transport::kMaxReceiveBatchMessages, frame_capacity})),
        .max_bytes = std::min<size_t>(remaining,
                                      transport::kMaxReceiveBatchBytes),
        .timeout_ms = 0,
        .connection_id = connection_id_,
    });
    if (!received.ok()) {
        return IsWouldBlock(received.status()) ? Status::Ok()
                                               : received.status();
    }
    for (transport::ReceivedMessage& message : received->messages) {
        auto inspected = WireFrameCodec::InspectHeader(
            message.payload, options_.wire_limits);
        if (!inspected.ok()) return inspected.status();
        MINO_RETURN_IF_ERROR(AuthorizeInboundData(*inspected));
        auto decoded = WireFrameCodec::Decode(message.payload,
                                              options_.wire_limits);
        if (!decoded.ok()) return decoded.status();
        const size_t wire_bytes = message.payload.size();
        MINO_RETURN_IF_ERROR(
            QueuePendingInbound(std::move(*decoded), wire_bytes));
        result->bytes += wire_bytes;
        ++result->inbound_frames;
        result->made_progress = true;
    }
    return ProcessPendingInbound(budget, result);
}

Status BridgePipeline::HandleFrame(const WireFrame& frame, uint64_t now_ns,
                                   BridgePumpResult*) noexcept {
    if (frame.header.frame_type == FrameType::kSessionHello) {
        return HandleHello(frame, now_ns);
    }
    if (!session_ready_) {
        return Corruption("bridge frame arrived before session handshake");
    }
    if (frame.header.frame_type == FrameType::kAck) {
        return HandleAck(frame);
    }
    if (frame.header.frame_type == FrameType::kData) {
        const SourceIdentity source = SourceFrom(frame.header);
        if (!ValidSource(source)) {
            return Corruption("bridge data source identity is incomplete");
        }
        if (!SourceBelongsToLane(source, options_.lane_index,
                                 options_.lane_count)) {
            return Corruption("bridge data source belongs to another lane");
        }
        if (schema_negotiator_ == nullptr) {
            return HandleData(frame, now_ns);
        }
        if (pending_schema_controls_.size() >=
            options_.max_control_frames) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "bridge pending schema control queue is full");
        }
        auto negotiated =
            schema_negotiator_->HandleDataFrame(frame, now_ns);
        if (!negotiated.ok()) return negotiated.status();
        MINO_RETURN_IF_ERROR(
            QueueNegotiatedControls(negotiated->outbound_control_frames));
        if (negotiated->ready_frames.size() > 1) {
            return Corruption("one data frame resolved to multiple frames");
        }
        return negotiated->ready_frames.empty()
                   ? Status::Ok()
                   : HandleData(negotiated->ready_frames.front(), now_ns);
    }
    if (frame.header.frame_type == FrameType::kSchemaAnnounce ||
        frame.header.frame_type == FrameType::kSchemaRequest) {
        if (schema_negotiator_ == nullptr) {
            return Corruption("schema control frame has no negotiator");
        }
        if (pending_schema_controls_.size() >=
            options_.max_control_frames) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "bridge pending schema control queue is full");
        }
        if (frame.header.frame_type == FrameType::kSchemaAnnounce) {
            const size_t retained_frames = pending_inbound_.size() - 1;
            const size_t retained_bytes =
                pending_inbound_bytes_ - processing_inbound_wire_bytes_;
            if (schema_negotiator_->buffered_frames() >
                    options_.max_pending_inbound_frames - retained_frames ||
                schema_negotiator_->buffered_bytes() >
                    options_.max_pending_inbound_bytes - retained_bytes) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "schema-ready batch has no inbound reservation");
            }
        }
        auto negotiated =
            schema_negotiator_->HandleControlFrame(frame, now_ns);
        if (!negotiated.ok()) return negotiated.status();
        MINO_RETURN_IF_ERROR(
            QueueNegotiatedControls(negotiated->outbound_control_frames));
        return StageReadyFrames(&negotiated->ready_frames);
    }
    // Heartbeats are consumed by TCP; receiving one from another transport is
    // harmless and does not affect protocol state.
    if (frame.header.frame_type == FrameType::kHeartbeat) return Status::Ok();
    return Corruption("bridge control frame type is unsupported");
}

Status BridgePipeline::HandleData(const WireFrame& frame,
                                  uint64_t now_ns) noexcept {
    const SourceIdentity source = SourceFrom(frame.header);
    if (!ValidSource(source)) {
        return Corruption("bridge data source identity is incomplete");
    }
    if (!SourceBelongsToLane(source, options_.lane_index,
                             options_.lane_count)) {
        return Corruption("bridge data source belongs to another lane");
    }
    auto checked = dedup_->Check(options_.remote_session_epoch, source,
                                 frame.header.sequence_num, now_ns);
    if (!checked.ok()) return checked.status();
    if (checked->decision == DedupDecision::kStaleSession) {
        return Corruption("bridge data belongs to a stale session");
    }
    if (checked->decision == DedupDecision::kDuplicateAccepted) {
        return EmitAck(frame, *checked, AckDisposition::kAccepted);
    }
    const bool degraded_baseline =
        checked->decision == DedupDecision::kNackWithHighest &&
        options_.local_dedup_state_lost &&
        checked->highest_contiguous_sequence.value_or(0) == 0 &&
        !dedup_->HasSource(source);
    if (checked->decision == DedupDecision::kNackWithHighest &&
        !degraded_baseline) {
        return EmitAck(frame, *checked, AckDisposition::kNackWithHighest);
    }

    uint64_t prospective_highest =
        checked->highest_contiguous_sequence.value_or(0);
    if (degraded_baseline ||
        (prospective_highest != std::numeric_limits<uint64_t>::max() &&
         frame.header.sequence_num == prospective_highest + 1)) {
        prospective_highest = frame.header.sequence_num;
    }
    if (!CanQueueAck(AckPayload{
            .sender_session_epoch = options_.local_session_epoch,
            .receiver_session_epoch = options_.remote_session_epoch,
            .source = source,
            .observed_sequence = frame.header.sequence_num,
            .highest_contiguous_sequence = prospective_highest,
            .disposition = AckDisposition::kAccepted,
        })) {
        return Status::Error(StatusCode::kWouldBlock,
                             "bridge pending ACK queue is full");
    }

    MINO_RETURN_IF_ERROR(dedup_->PrepareSource(
        options_.remote_session_epoch, source, now_ns));
    MINO_RETURN_IF_ERROR(ingress_->DecodeValidatePublish(frame));
    if (degraded_baseline) {
        MINO_RETURN_IF_ERROR(dedup_->SeedAccepted(
            options_.remote_session_epoch, source,
            frame.header.sequence_num, now_ns));
    } else {
        MINO_RETURN_IF_ERROR(dedup_->CommitAccepted(
            options_.remote_session_epoch, source,
            frame.header.sequence_num, now_ns));
    }
    auto committed = dedup_->Check(options_.remote_session_epoch, source,
                                   frame.header.sequence_num, now_ns);
    if (!committed.ok()) return committed.status();
    return EmitAck(frame, *committed, AckDisposition::kAccepted);
}

Status BridgePipeline::EmitAck(const WireFrame& data,
                               const DedupCheckResult& state,
                               AckDisposition disposition) noexcept {
    AckPayload ack{
        .sender_session_epoch = options_.local_session_epoch,
        .receiver_session_epoch = options_.remote_session_epoch,
        .source = SourceFrom(data.header),
        .observed_sequence = data.header.sequence_num,
        .highest_contiguous_sequence =
            state.highest_contiguous_sequence,
        .disposition = disposition,
    };
    return QueueAck(ack);
}

Status BridgePipeline::AddAttempt(Attempt attempt) noexcept {
    try {
        const AttemptKey key{
            .source = attempt.source,
            .sequence = attempt.sequence,
            .connection_id = attempt.operation.connection_id,
            .operation_id = attempt.operation.id,
        };
        auto inserted = attempts_.emplace(key, std::move(attempt));
        if (!inserted.second) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "bridge send attempt already exists");
        }
        try {
            if (!attempt_operations_
                     .emplace(inserted.first->second.operation, key)
                     .second) {
                attempts_.erase(inserted.first);
                return Status::Error(StatusCode::kAlreadyExists,
                                     "bridge send operation already exists");
            }
        } catch (const std::bad_alloc&) {
            attempts_.erase(inserted.first);
            return AllocationFailure();
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

void BridgePipeline::RemoveAttempt(AttemptMap::iterator attempt) noexcept {
    attempt_operations_.erase(attempt->second.operation);
    attempts_.erase(attempt);
}

BridgePipeline::AttemptMap::iterator BridgePipeline::FindAttempt(
    const SourceIdentity& source, uint64_t sequence,
    transport::ConnectionId connection_id) noexcept {
    auto attempt = attempts_.lower_bound(AttemptKey{
        .source = source,
        .sequence = sequence,
        .connection_id = transport::kInvalidConnectionId,
        .operation_id = transport::kInvalidOperationId,
    });
    while (attempt != attempts_.end() && attempt->first.source == source &&
           attempt->first.sequence == sequence) {
        if (attempt->first.connection_id == connection_id) return attempt;
        ++attempt;
    }
    return attempts_.end();
}

Status BridgePipeline::RetireAcknowledgedAttempts(
    const AckPayload& ack) noexcept {
    const auto retire = [this](AttemptMap::iterator attempt) -> Status {
        const Status confirmed =
            driver_->ConfirmRemoteAccepted(attempt->second.operation);
        if (!confirmed.ok() && confirmed.code() != StatusCode::kNotFound) {
            return confirmed;
        }
        if (!confirmed.ok() &&
            attempt->second.operation.connection_id != connection_id_) {
            return Status::Ok();
        }
        RemoveAttempt(attempt);
        return Status::Ok();
    };

    const uint64_t highest = ack.highest_contiguous_sequence.value_or(0);
    if (ack.highest_contiguous_sequence.has_value()) {
        auto attempt = attempts_.lower_bound(AttemptKey{
            .source = ack.source,
            .sequence = 0,
            .connection_id = transport::kInvalidConnectionId,
            .operation_id = transport::kInvalidOperationId,
        });
        while (attempt != attempts_.end() &&
               attempt->first.source == ack.source &&
               attempt->first.sequence <= highest) {
            const auto current = attempt++;
            MINO_RETURN_IF_ERROR(retire(current));
        }
    }
    if (ack.disposition == AckDisposition::kAccepted &&
        ack.observed_sequence > highest) {
        auto attempt = attempts_.lower_bound(AttemptKey{
            .source = ack.source,
            .sequence = ack.observed_sequence,
            .connection_id = transport::kInvalidConnectionId,
            .operation_id = transport::kInvalidOperationId,
        });
        while (attempt != attempts_.end() &&
               attempt->first.source == ack.source &&
               attempt->first.sequence == ack.observed_sequence) {
            const auto current = attempt++;
            MINO_RETURN_IF_ERROR(retire(current));
        }
    }
    return Status::Ok();
}

Status BridgePipeline::HandleAck(const WireFrame& frame) noexcept {
    try {
        auto ack = ControlPayloadCodec::DecodeAck(frame.payload);
        if (!ack.ok()) return ack.status();
        if (!SourceBelongsToLane(ack->source, options_.lane_index,
                                 options_.lane_count)) {
            return Corruption("bridge ACK source belongs to another lane");
        }
        auto applied = retransmit_->ApplyAck(*ack);
        if (!applied.ok()) return applied.status();
        RemoveAcknowledgedReliable(*ack);
        if (ack->disposition == AckDisposition::kNackWithHighest) {
            const uint64_t highest =
                ack->highest_contiguous_sequence.value_or(0);
            auto attempt = attempts_.lower_bound(AttemptKey{
                .source = ack->source,
                .sequence = highest == std::numeric_limits<uint64_t>::max()
                                ? highest
                                : highest + 1,
                .connection_id = transport::kInvalidConnectionId,
                .operation_id = transport::kInvalidOperationId,
            });
            while (attempt != attempts_.end() &&
                   attempt->first.source == ack->source) {
                Attempt& pending = attempt->second;
                if (pending.operation.connection_id == connection_id_ &&
                    pending.sequence > highest &&
                    retransmit_->Find(pending.source, pending.sequence) !=
                        nullptr) {
                    pending.retry_requested = true;
                    resend_pending_ = true;
                }
                ++attempt;
            }
        }
        MINO_RETURN_IF_ERROR(RetireAcknowledgedAttempts(*ack));
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::HandleHello(const WireFrame& frame,
                                   uint64_t) noexcept {
    try {
        auto hello = ControlPayloadCodec::DecodeSessionHello(frame.payload);
        if (!hello.ok()) return hello.status();
        if (hello->sender_session_epoch != options_.remote_session_epoch ||
            hello->receiver_session_epoch != options_.local_session_epoch) {
            return Corruption("session hello epoch fencing failed");
        }
        for (const SessionHelloSource& source : hello->sources) {
            if (!SourceBelongsToLane(source.source, options_.lane_index,
                                     options_.lane_count)) {
                return Corruption(
                    "session hello source belongs to another lane");
            }
        }

        peer_dedup_state_lost_ = hello->dedup_state_lost;
        if (!hello->dedup_state_lost) {
            for (const SessionHelloSource& source : hello->sources) {
                const AckPayload resume_ack{
                    .sender_session_epoch = options_.remote_session_epoch,
                    .receiver_session_epoch = options_.local_session_epoch,
                    .source = source.source,
                    .observed_sequence = 0,
                    .highest_contiguous_sequence =
                        source.last_accepted_sequence,
                    .disposition = AckDisposition::kAccepted,
                };
                auto applied = retransmit_->ApplyAck(resume_ack);
                if (!applied.ok()) return applied.status();
                RemoveAcknowledgedReliable(resume_ack);
                MINO_RETURN_IF_ERROR(
                    RetireAcknowledgedAttempts(resume_ack));
            }
        }
        RemoveRetiredReliable();
        resend_pending_ = retransmit_->size() != 0;
        hello_received_ = true;
        session_ready_ = hello_sent_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status BridgePipeline::AddPendingReliable(
    PendingReliable pending) noexcept {
    try {
        const ReliableKey key{pending.source, pending.sequence};
        if (pending_reliable_index_.contains(key)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "bridge pending reliable frame already exists");
        }
        pending_reliable_.push_back(std::move(pending));
        const size_t index = pending_reliable_.size() - 1;
        auto source = pending_reliable_sources_.end();
        bool source_created = false;
        bool sequence_inserted = false;
        bool key_inserted = false;
        try {
            auto source_result = pending_reliable_sources_.try_emplace(
                pending_reliable_[index].source);
            source = source_result.first;
            source_created = source_result.second;
            sequence_inserted =
                source->second
                    .emplace(pending_reliable_[index].sequence, index)
                    .second;
            key_inserted = pending_reliable_index_.emplace(key, index).second;
            if (!sequence_inserted || !key_inserted) {
                if (key_inserted) pending_reliable_index_.erase(key);
                if (sequence_inserted) {
                    source->second.erase(pending_reliable_[index].sequence);
                }
                if (source_created && source->second.empty()) {
                    pending_reliable_sources_.erase(source);
                }
                pending_reliable_.pop_back();
                return Status::Error(
                    StatusCode::kAlreadyExists,
                    "bridge pending reliable frame already exists");
            }
        } catch (const std::bad_alloc&) {
            if (key_inserted) pending_reliable_index_.erase(key);
            if (sequence_inserted) {
                source->second.erase(pending_reliable_[index].sequence);
            }
            if (source_created &&
                source != pending_reliable_sources_.end() &&
                source->second.empty()) {
                pending_reliable_sources_.erase(source);
            }
            pending_reliable_.pop_back();
            return AllocationFailure();
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

BridgePipeline::PendingReliable* BridgePipeline::FindPendingReliable(
    const SourceIdentity& source, uint64_t sequence) noexcept {
    const auto found =
        pending_reliable_index_.find(ReliableKey{source, sequence});
    return found == pending_reliable_index_.end()
               ? nullptr
               : &pending_reliable_[found->second];
}

void BridgePipeline::RemovePendingReliableAt(size_t index) noexcept {
    const ReliableKey removed{pending_reliable_[index].source,
                              pending_reliable_[index].sequence};
    pending_reliable_index_.erase(removed);
    const auto source = pending_reliable_sources_.find(removed.source);
    if (source != pending_reliable_sources_.end()) {
        source->second.erase(removed.sequence);
        if (source->second.empty()) pending_reliable_sources_.erase(source);
    }

    const size_t last = pending_reliable_.size() - 1;
    if (index != last) {
        pending_reliable_[index] = std::move(pending_reliable_[last]);
        const ReliableKey moved{pending_reliable_[index].source,
                                pending_reliable_[index].sequence};
        const auto moved_index = pending_reliable_index_.find(moved);
        if (moved_index != pending_reliable_index_.end()) {
            moved_index->second = index;
        }
        const auto moved_source =
            pending_reliable_sources_.find(moved.source);
        if (moved_source != pending_reliable_sources_.end()) {
            const auto moved_sequence =
                moved_source->second.find(moved.sequence);
            if (moved_sequence != moved_source->second.end()) {
                moved_sequence->second = index;
            }
        }
    }
    pending_reliable_.pop_back();
}

void BridgePipeline::RemoveAcknowledgedReliable(
    const AckPayload& ack) noexcept {
    const uint64_t highest = ack.highest_contiguous_sequence.value_or(0);
    if (ack.highest_contiguous_sequence.has_value()) {
        while (true) {
            const auto source = pending_reliable_sources_.find(ack.source);
            if (source == pending_reliable_sources_.end() ||
                source->second.empty() ||
                source->second.begin()->first > highest) {
                break;
            }
            const uint64_t sequence = source->second.begin()->first;
            if (retransmit_->Find(ack.source, sequence) != nullptr) break;
            RemovePendingReliableAt(source->second.begin()->second);
        }
    }
    if (ack.disposition == AckDisposition::kAccepted &&
        ack.observed_sequence > highest) {
        const auto found = pending_reliable_index_.find(
            ReliableKey{ack.source, ack.observed_sequence});
        if (found != pending_reliable_index_.end() &&
            retransmit_->Find(ack.source, ack.observed_sequence) == nullptr) {
            RemovePendingReliableAt(found->second);
        }
    }
}

void BridgePipeline::RemoveRetiredReliable() noexcept {
    for (size_t i = 0; i < pending_reliable_.size();) {
        const PendingReliable& pending = pending_reliable_[i];
        if (retransmit_->Find(pending.source, pending.sequence) != nullptr) {
            ++i;
            continue;
        }
        RemovePendingReliableAt(i);
    }
}

Status BridgePipeline::PrepareSchema(EncodedOutboundFrame* outbound,
                                     bool* control_pending) noexcept {
    *control_pending = false;
    if (schema_negotiator_ == nullptr) return Status::Ok();
    if (!outbound->schema_identity.has_value()) {
        return Invalid("schema-aware egress omitted full schema identity");
    }
    const auto existing = std::find_if(
        local_schema_bindings_.begin(), local_schema_bindings_.end(),
        [&outbound](const LocalSchemaBinding& binding) {
            return registry::SchemaIdentityEqual(
                binding.identity, *outbound->schema_identity);
        });
    if (existing != local_schema_bindings_.end()) {
        outbound->frame.header.connection_schema_ref =
            existing->connection_ref;
        return Status::Ok();
    }

    MINO_ASSIGN_OR_RETURN(
        auto announcement,
        schema_negotiator_->BindLocalSchema(
            *outbound->schema_identity, outbound->descriptor_artifact));
    MINO_ASSIGN_OR_RETURN(
        auto decoded,
        SchemaControlCodec::DecodeAnnouncement(announcement.payload));
    MINO_RETURN_IF_ERROR(QueueControl(announcement));
    local_schema_bindings_.push_back(LocalSchemaBinding{
        .identity = *outbound->schema_identity,
        .connection_ref = decoded.connection_schema_ref,
    });
    outbound->frame.header.connection_schema_ref =
        decoded.connection_schema_ref;
    *control_pending = true;
    return Status::Ok();
}

Status BridgePipeline::ResendPending(const BridgePumpBudget& budget,
                                     BridgePumpResult* result) noexcept {
    if (!session_ready_ || !resend_pending_) return Status::Ok();
    for (const RetransmitEntry& entry : retransmit_->entries()) {
        const auto existing_attempt =
            FindAttempt(entry.source, entry.sequence, connection_id_);
        if (existing_attempt != attempts_.end() &&
            !existing_attempt->second.retry_requested) {
            continue;
        }

        std::span<const std::byte> body = entry.frame;
        std::vector<std::byte> rebound_body;
        PendingReliable* pending =
            FindPendingReliable(entry.source, entry.sequence);
        const bool retransmission =
            pending != nullptr && pending->transmitted;
        if (schema_negotiator_ != nullptr) {
            if (pending == nullptr) {
                return Corruption(
                    "schema-aware retransmit omitted logical frame");
            }
            bool control_pending = false;
            MINO_RETURN_IF_ERROR(
                PrepareSchema(&pending->outbound, &control_pending));
            if (control_pending) return Status::Ok();
            MINO_ASSIGN_OR_RETURN(
                rebound_body,
                WireFrameCodec::Encode(pending->outbound.frame,
                                       options_.wire_limits));
            body = rebound_body;
        }
        if (result->outbound_frames >= budget.max_outbound_frames ||
            body.size() > budget.max_bytes - result->bytes ||
            (existing_attempt == attempts_.end() &&
             attempts_.size() >= options_.retransmit.max_entries)) {
            return Status::Ok();
        }
        if (existing_attempt != attempts_.end()) {
            auto sent = driver_->SendUntracked(
                transport::UntrackedSendRequest{
                    .connection_id = connection_id_,
                    .payload = body,
                    .traffic_class =
                        transport::UntrackedTrafficClass::kData,
                });
            if (!sent.ok()) {
                return IsWouldBlock(sent.status()) ? Status::Ok()
                                                   : sent.status();
            }
            existing_attempt->second.retry_requested = false;
        } else {
            auto sent = driver_->Send(transport::SendRequest{
                .connection_id = connection_id_,
                .payload = body,
                .target_stage = DeliveryStage::kRemoteAccepted,
            });
            if (!sent.ok()) {
                return IsWouldBlock(sent.status()) ? Status::Ok()
                                                   : sent.status();
            }
            MINO_RETURN_IF_ERROR(AddAttempt(Attempt{
                .source = entry.source,
                .sequence = entry.sequence,
                .operation = sent->operation,
                .retry_requested = false,
            }));
        }
        if (pending != nullptr) pending->transmitted = true;
        result->bytes += body.size();
        ++result->outbound_frames;
        if (retransmission) ++result->retransmitted_frames;
        result->made_progress = true;
    }
    resend_pending_ = false;
    return Status::Ok();
}

Status BridgePipeline::PullOutbound(const BridgePumpBudget& budget,
                                    BridgePumpResult* result) noexcept {
    if (!session_ready_ || egress_ == nullptr) return Status::Ok();
    while (result->outbound_frames < budget.max_outbound_frames &&
           result->bytes < budget.max_bytes) {
        auto outbound = egress_->TryPeekAndEncode();
        if (!outbound.ok()) {
            return IsWouldBlock(outbound.status()) ? Status::Ok()
                                                   : outbound.status();
        }
        bool ownership_transferred = false;
        const Status sent = SendData(std::move(*outbound), budget.now_ns,
                                     budget, result,
                                     &ownership_transferred);
        if (ownership_transferred) egress_->CommitPolled();
        if (!sent.ok()) {
            return IsWouldBlock(sent) ? Status::Ok() : sent;
        }
    }
    return Status::Ok();
}

Status BridgePipeline::SendData(EncodedOutboundFrame outbound,
                                uint64_t now_ns,
                                const BridgePumpBudget& budget,
                                BridgePumpResult* result,
                                bool* ownership_transferred) noexcept {
    *ownership_transferred = false;
    if (outbound.frame.header.frame_type != FrameType::kData) {
        return Invalid("egress port returned a non-data frame");
    }
    const SourceIdentity source = SourceFrom(outbound.frame.header);
    if (!ValidSource(source)) {
        return Invalid("egress source identity is incomplete");
    }
    if (!SourceBelongsToLane(source, options_.lane_index,
                             options_.lane_count)) {
        return Invalid("egress source belongs to another bridge lane");
    }
    bool schema_control_pending = false;
    MINO_RETURN_IF_ERROR(
        PrepareSchema(&outbound, &schema_control_pending));
    if (schema_control_pending) {
        return Status::Error(StatusCode::kWouldBlock,
                             "schema announcement must be flushed first");
    }
    MINO_ASSIGN_OR_RETURN(
        auto body,
        WireFrameCodec::Encode(outbound.frame, options_.wire_limits));
    if (body.size() > options_.wire_limits.max_buffered_bytes) {
        return Exhausted("encoded bridge frame exceeds wire limit");
    }
    if (body.size() > budget.max_bytes - result->bytes) {
        return Status::Error(StatusCode::kWouldBlock,
                             "encoded bridge frame exceeds remaining budget");
    }

    if (outbound.reliability == registry::Reliability::kReliableOrdered) {
        if (!driver_->capabilities().features.Has(
                transport::Capability::kRemoteAcceptedConfirmation)) {
            return Status::Error(
                StatusCode::kUnsupported,
                "reliable Bridge requires remote ACK confirmation");
        }
        if (body.size() > options_.retransmit.max_bytes) {
            return Exhausted("encoded frame exceeds retransmit byte limit");
        }
        if (attempts_.size() >= options_.retransmit.max_entries) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "retired send attempts await completion");
        }
        if (retransmit_->size() >= options_.retransmit.max_entries ||
            body.size() > options_.retransmit.max_bytes -
                              retransmit_->bytes()) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "retransmit window is full");
        }
        const uint64_t sequence = outbound.frame.header.sequence_num;
        MINO_RETURN_IF_ERROR(AddPendingReliable(PendingReliable{
            .source = source,
            .sequence = sequence,
            .outbound = std::move(outbound),
        }));
        const Status admitted =
            retransmit_->Add(source, sequence, body, now_ns);
        if (!admitted.ok()) {
            const auto pending =
                pending_reliable_index_.find(ReliableKey{source, sequence});
            if (pending != pending_reliable_index_.end()) {
                RemovePendingReliableAt(pending->second);
            }
            return admitted;
        }
        PendingReliable* pending = FindPendingReliable(source, sequence);
        if (pending == nullptr) {
            return Corruption("admitted reliable frame lost its logical index");
        }
        *ownership_transferred = true;
        auto sent = driver_->Send(transport::SendRequest{
            .connection_id = connection_id_,
            .payload = body,
            .target_stage = DeliveryStage::kRemoteAccepted,
        });
        if (!sent.ok()) {
            resend_pending_ = true;
            // The frame is retained in the retransmit window, but later
            // sequences must not overtake it when the driver queue is full.
            // Propagate backpressure so PullOutbound stops this round and the
            // next Pump retries retained frames first.
            return sent.status();
        }
        pending->transmitted = true;
        MINO_RETURN_IF_ERROR(AddAttempt(Attempt{
            .source = source,
            .sequence = sequence,
            .operation = sent->operation,
            .retry_requested = false,
        }));
    } else {
        auto sent = driver_->SendUntracked(transport::UntrackedSendRequest{
            .connection_id = connection_id_,
            .payload = body,
            .traffic_class = transport::UntrackedTrafficClass::kData,
        });
        if (!sent.ok()) {
            if (IsWouldBlock(sent.status()) && outbound.allow_drop) {
                *ownership_transferred = true;
                return Status::Ok();
            }
            return sent.status();
        }
        *ownership_transferred = true;
    }
    result->bytes += body.size();
    ++result->outbound_frames;
    result->made_progress = true;
    return Status::Ok();
}

Result<BridgePumpResult> BridgePipeline::Pump(
    const BridgePumpBudget& budget) noexcept {
    try {
        if (budget.max_inbound_frames > transport::kMaxReceiveBatchMessages ||
            budget.max_outbound_frames == 0 || budget.max_bytes == 0 ||
            budget.max_bytes > transport::kMaxReceiveBatchBytes) {
            return Invalid("bridge pump budget is invalid");
        }
        BridgePumpResult result;
        MINO_RETURN_IF_ERROR(AdmitNegotiatedControls());
        MINO_RETURN_IF_ERROR(FlushControls(budget, &result));
        MINO_RETURN_IF_ERROR(FlushAcks(budget, &result));
        MINO_RETURN_IF_ERROR(DrainCompletions(budget, &result));
        MINO_RETURN_IF_ERROR(DrainInbound(budget, &result));
        MINO_RETURN_IF_ERROR(AdmitNegotiatedControls());
        MINO_RETURN_IF_ERROR(FlushControls(budget, &result));
        MINO_RETURN_IF_ERROR(FlushAcks(budget, &result));
        if (control_queue_.empty()) {
            MINO_RETURN_IF_ERROR(ResendPending(budget, &result));
            MINO_RETURN_IF_ERROR(PullOutbound(budget, &result));
        }
        MINO_RETURN_IF_ERROR(FlushControls(budget, &result));
        MINO_RETURN_IF_ERROR(FlushAcks(budget, &result));
        const size_t expired = retransmit_->PurgeExpired(budget.now_ns);
        if (expired != 0) RemoveRetiredReliable();
        dedup_->PurgeExpired(budget.now_ns);
        if (expired != 0) {
            const Status closed = driver_->Close(connection_id_);
            attempts_.clear();
            attempt_operations_.clear();
            session_ready_ = false;
            if (!closed.ok() && closed.code() != StatusCode::kNotFound) {
                return closed;
            }
            return Status::Error(
                StatusCode::kTimeout,
                "Bridge reliable frame exceeded retransmit retention");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

}  // namespace mino::bridge
