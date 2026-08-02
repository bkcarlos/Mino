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
            options.max_pending_inbound_bytes == 0) {
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
        pipeline->attempts_.reserve(options.retransmit.max_entries);
        pipeline->pending_reliable_.reserve(options.retransmit.max_entries);
        pipeline->pending_inbound_.reserve(
            options.max_pending_inbound_frames);
        pipeline->staged_ready_.reserve(
            options.max_pending_inbound_frames);
        pipeline->pending_acks_.reserve(options.max_control_frames);
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
    uint64_t now_ns) noexcept {
    try {
        if (connection_id == transport::kInvalidConnectionId ||
            local_session_epoch == 0 || remote_session_epoch == 0) {
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
        control_queue_.clear();
        control_bytes_ = 0;
        pending_acks_.clear();
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

Status BridgePipeline::QueueAck(const AckPayload& ack) noexcept {
    try {
        const auto existing = std::find_if(
            pending_acks_.begin(), pending_acks_.end(),
            [&ack](const AckPayload& pending) {
                return pending.source == ack.source &&
                       pending.observed_sequence == ack.observed_sequence &&
                       pending.sender_session_epoch ==
                           ack.sender_session_epoch &&
                       pending.receiver_session_epoch ==
                           ack.receiver_session_epoch;
            });
        if (existing != pending_acks_.end()) {
            *existing = ack;
            return Status::Ok();
        }
        if (pending_acks_.size() >= options_.max_control_frames) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "bridge pending ACK queue is full");
        }
        pending_acks_.push_back(ack);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
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
        pending_acks_.erase(pending_acks_.begin());
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
        for (const Attempt& attempt : attempts_) {
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
                const auto attempt = std::find_if(
                    attempts_.begin(), attempts_.end(),
                    [&completion](const Attempt& candidate) {
                        return candidate.operation == completion.operation;
                    });
                if (attempt != attempts_.end()) {
                    if (!completion.status.ok() &&
                        completion.operation.connection_id == connection_id_) {
                        current_failure = completion.status;
                        resend_pending_ = true;
                        session_ready_ = false;
                    }
                    attempts_.erase(attempt);
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

    const bool ack_already_pending = std::any_of(
        pending_acks_.begin(), pending_acks_.end(),
        [&source, &frame](const AckPayload& pending) {
            return pending.source == source &&
                   pending.observed_sequence == frame.header.sequence_num;
        });
    if (!ack_already_pending &&
        pending_acks_.size() >= options_.max_control_frames) {
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

Status BridgePipeline::RetireAcknowledgedAttempts(
    const std::vector<Attempt>& before) noexcept {
    for (const Attempt& attempt : before) {
        if (retransmit_->Find(attempt.source, attempt.sequence) != nullptr) {
            continue;
        }
        const Status confirmed =
            driver_->ConfirmRemoteAccepted(attempt.operation);
        if (!confirmed.ok() && confirmed.code() != StatusCode::kNotFound) {
            return confirmed;
        }
        if (!confirmed.ok() &&
            attempt.operation.connection_id != connection_id_) {
            continue;
        }
        attempts_.erase(
            std::remove_if(attempts_.begin(), attempts_.end(),
                           [&attempt](const Attempt& current) {
                               return current.operation == attempt.operation;
                           }),
            attempts_.end());
    }
    return Status::Ok();
}

Status BridgePipeline::HandleAck(const WireFrame& frame) noexcept {
    try {
        auto ack = ControlPayloadCodec::DecodeAck(frame.payload);
        if (!ack.ok()) return ack.status();
        const std::vector<Attempt> before = attempts_;
        auto applied = retransmit_->ApplyAck(*ack);
        if (!applied.ok()) return applied.status();
        if (ack->disposition == AckDisposition::kNackWithHighest) {
            const uint64_t highest =
                ack->highest_contiguous_sequence.value_or(0);
            for (Attempt& attempt : attempts_) {
                if (attempt.operation.connection_id == connection_id_ &&
                    attempt.source == ack->source &&
                    attempt.sequence > highest &&
                    retransmit_->Find(attempt.source, attempt.sequence) !=
                        nullptr) {
                    attempt.retry_requested = true;
                    resend_pending_ = true;
                }
            }
        }
        MINO_RETURN_IF_ERROR(RetireAcknowledgedAttempts(before));
        RemoveRetiredReliable();
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

        peer_dedup_state_lost_ = hello->dedup_state_lost;
        const std::vector<Attempt> before = attempts_;
        if (!hello->dedup_state_lost) {
            for (const SessionHelloSource& source : hello->sources) {
                auto applied = retransmit_->ApplyAck(AckPayload{
                    .sender_session_epoch = options_.remote_session_epoch,
                    .receiver_session_epoch = options_.local_session_epoch,
                    .source = source.source,
                    .observed_sequence = 0,
                    .highest_contiguous_sequence =
                        source.last_accepted_sequence,
                    .disposition = AckDisposition::kAccepted,
                });
                if (!applied.ok()) return applied.status();
            }
        }
        MINO_RETURN_IF_ERROR(RetireAcknowledgedAttempts(before));
        RemoveRetiredReliable();
        resend_pending_ = retransmit_->size() != 0;
        hello_received_ = true;
        session_ready_ = hello_sent_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

BridgePipeline::PendingReliable* BridgePipeline::FindPendingReliable(
    const SourceIdentity& source, uint64_t sequence) noexcept {
    const auto found = std::find_if(
        pending_reliable_.begin(), pending_reliable_.end(),
        [&source, sequence](const PendingReliable& pending) {
            return pending.source == source && pending.sequence == sequence;
        });
    return found == pending_reliable_.end() ? nullptr : &*found;
}

void BridgePipeline::RemoveRetiredReliable() noexcept {
    pending_reliable_.erase(
        std::remove_if(
            pending_reliable_.begin(), pending_reliable_.end(),
            [this](const PendingReliable& pending) {
                return retransmit_->Find(pending.source, pending.sequence) ==
                       nullptr;
            }),
        pending_reliable_.end());
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
        const auto existing_attempt = std::find_if(
            attempts_.begin(), attempts_.end(),
            [this, &entry](const Attempt& attempt) {
                return attempt.operation.connection_id == connection_id_ &&
                       attempt.source == entry.source &&
                       attempt.sequence == entry.sequence;
            });
        if (existing_attempt != attempts_.end() &&
            !existing_attempt->retry_requested) {
            continue;
        }

        std::span<const std::byte> body = entry.frame;
        std::vector<std::byte> rebound_body;
        PendingReliable* pending =
            FindPendingReliable(entry.source, entry.sequence);
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
            existing_attempt->retry_requested = false;
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
            attempts_.push_back(Attempt{
                .source = entry.source,
                .sequence = entry.sequence,
                .operation = sent->operation,
                .retry_requested = false,
            });
        }
        result->bytes += body.size();
        ++result->outbound_frames;
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
        MINO_RETURN_IF_ERROR(retransmit_->Add(
            source, outbound.frame.header.sequence_num, body, now_ns));
        pending_reliable_.push_back(PendingReliable{
            .source = source,
            .sequence = outbound.frame.header.sequence_num,
            .outbound = std::move(outbound),
        });
        *ownership_transferred = true;
        auto sent = driver_->Send(transport::SendRequest{
            .connection_id = connection_id_,
            .payload = body,
            .target_stage = DeliveryStage::kRemoteAccepted,
        });
        if (!sent.ok()) {
            resend_pending_ = true;
            return IsWouldBlock(sent.status()) ? Status::Ok() : sent.status();
        }
        attempts_.push_back(Attempt{
            .source = source,
            .sequence = pending_reliable_.back().sequence,
            .operation = sent->operation,
            .retry_requested = false,
        });
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
        RemoveRetiredReliable();
        dedup_->PurgeExpired(budget.now_ns);
        if (expired != 0) {
            const Status closed = driver_->Close(connection_id_);
            attempts_.clear();
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
