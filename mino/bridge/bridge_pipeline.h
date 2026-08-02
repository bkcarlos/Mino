// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_BRIDGE_PIPELINE_H_
#define MINO_BRIDGE_BRIDGE_PIPELINE_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <vector>

#include "mino/bridge/dedup_window.h"
#include "mino/bridge/retransmit_window.h"
#include "mino/bridge/schema_negotiator.h"
#include "mino/bridge/wire_frame.h"
#include "mino/common/result.h"
#include "mino/registry/metadata.h"
#include "mino/transport/transport_driver.h"

namespace mino::bridge {

struct EncodedOutboundFrame {
    WireFrame frame;
    registry::Reliability reliability = registry::Reliability::kBestEffort;
    bool allow_drop = false;
    // Required when a SchemaNegotiator is attached. The connection-local ref is
    // assigned by BridgePipeline and is never reused across RebindConnection().
    std::optional<schema::SchemaIdentity> schema_identity;
    std::vector<std::byte> descriptor_artifact;
};

class BridgeEgressPort {
public:
    virtual ~BridgeEgressPort() = default;
    // Peek is non-destructive. BridgePipeline calls CommitPolled only after it
    // has either admitted the frame to the reliable window, sent it, or applied
    // an explicit best-effort drop policy.
    virtual Result<EncodedOutboundFrame> TryPeekAndEncode() = 0;
    virtual void CommitPolled() noexcept = 0;
};

class BridgeIngressPort {
public:
    virtual ~BridgeIngressPort() = default;
    // Success means the decoded object is committed to the local publication
    // channel and the receiver may advance dedup and emit Accepted ACK.
    virtual Status DecodeValidatePublish(const WireFrame& frame) = 0;
};

struct BridgePipelineOptions {
    uint64_t local_session_epoch = 0;
    uint64_t remote_session_epoch = 0;
    // Set only when this endpoint cannot restore its receiver-side dedup state.
    // The peer then exposes a degraded, potentially duplicate-delivery session.
    bool local_dedup_state_lost = false;
    size_t max_control_frames = 1024;
    size_t max_control_bytes = 256u * 1024u;
    size_t max_pending_inbound_frames = 1024;
    size_t max_pending_inbound_bytes = 64u * 1024u * 1024u;
    WireFrameLimits wire_limits;
    DedupWindowOptions dedup;
    RetransmitWindowOptions retransmit;
};

struct BridgePumpBudget {
    uint32_t max_completions = 64;
    uint32_t max_inbound_frames = 64;
    uint32_t max_outbound_frames = 64;
    size_t max_bytes = 4u * 1024u * 1024u;
    uint64_t now_ns = 0;
};

struct BridgePumpResult {
    size_t completions = 0;
    size_t inbound_frames = 0;
    size_t outbound_frames = 0;
    size_t bytes = 0;
    bool made_progress = false;
};

class BridgePipeline final {
public:
    static Result<std::unique_ptr<BridgePipeline>> Create(
        BridgePipelineOptions options,
        std::shared_ptr<transport::TransportDriver> driver,
        transport::ConnectionId connection_id,
        BridgeEgressPort* egress,
        BridgeIngressPort* ingress,
        SchemaNegotiator* schema_negotiator = nullptr) noexcept;

    Result<BridgePumpResult> Pump(const BridgePumpBudget& budget) noexcept;

    // Rebinds the connection without discarding owned reliable frames. A normal
    // reconnect preserves dedup state; receiver restart explicitly clears it
    // and advertises the degraded path in SessionHello.
    Status RebindConnection(transport::ConnectionId connection_id,
                            uint64_t local_session_epoch,
                            uint64_t remote_session_epoch,
                            bool local_dedup_state_lost,
                            uint64_t now_ns) noexcept;

    bool session_ready() const noexcept { return session_ready_; }
    bool peer_dedup_state_lost() const noexcept {
        return peer_dedup_state_lost_;
    }
    bool reliability_degraded() const noexcept {
        return options_.local_dedup_state_lost || peer_dedup_state_lost_;
    }
    Status reliability_status() const;
    size_t retransmit_entries() const noexcept { return retransmit_->size(); }
    const RetransmitWindowStats& retransmit_stats() const noexcept {
        return retransmit_->stats();
    }
    const DedupWindowStats& dedup_stats() const noexcept {
        return dedup_->stats();
    }
    size_t control_queue_size() const noexcept { return control_queue_.size(); }
    size_t pending_inbound_frames() const noexcept {
        return pending_inbound_.size();
    }

private:
    struct Attempt {
        SourceIdentity source;
        uint64_t sequence = 0;
        transport::SendOperation operation;
        bool retry_requested = false;
    };

    struct PendingInbound {
        WireFrame frame;
        size_t wire_bytes = 0;
        bool schema_resolved = false;
    };

    struct PendingReliable {
        SourceIdentity source;
        uint64_t sequence = 0;
        EncodedOutboundFrame outbound;
    };

    struct LocalSchemaBinding {
        schema::SchemaIdentity identity{0, {}, 0, 0};
        uint32_t connection_ref = 0;
    };

    BridgePipeline(BridgePipelineOptions options,
                   std::shared_ptr<transport::TransportDriver> driver,
                   transport::ConnectionId connection_id,
                   BridgeEgressPort* egress, BridgeIngressPort* ingress,
                   SchemaNegotiator* schema_negotiator,
                   std::unique_ptr<DedupWindow> dedup,
                   std::unique_ptr<RetransmitWindow> retransmit) noexcept;

    Status QueueControl(const WireFrame& frame) noexcept;
    Status QueueNegotiatedControls(
        const std::vector<WireFrame>& controls) noexcept;
    Status AdmitNegotiatedControls() noexcept;
    Status QueueSessionHello() noexcept;
    Status QueueAck(const AckPayload& ack) noexcept;
    Status FlushAcks(BridgePumpBudget budget,
                     BridgePumpResult* result) noexcept;
    Status FlushControls(BridgePumpBudget budget,
                         BridgePumpResult* result) noexcept;
    Status DrainCompletions(const BridgePumpBudget& budget,
                            BridgePumpResult* result) noexcept;
    Status DrainInbound(const BridgePumpBudget& budget,
                        BridgePumpResult* result) noexcept;
    Status ProcessPendingInbound(const BridgePumpBudget& budget,
                                 BridgePumpResult* result) noexcept;
    Status QueuePendingInbound(WireFrame frame, size_t wire_bytes,
                               bool schema_resolved = false) noexcept;
    Status StageReadyFrames(std::vector<WireFrame>* frames) noexcept;
    Status HandleFrame(const WireFrame& frame, uint64_t now_ns,
                       BridgePumpResult* result) noexcept;
    Status HandleData(const WireFrame& frame, uint64_t now_ns) noexcept;
    Status HandleAck(const WireFrame& frame) noexcept;
    Status HandleHello(const WireFrame& frame, uint64_t now_ns) noexcept;
    Status EmitAck(const WireFrame& data, const DedupCheckResult& state,
                   AckDisposition disposition) noexcept;
    Status RetireAcknowledgedAttempts(
        const std::vector<Attempt>& before) noexcept;
    Status ResendPending(const BridgePumpBudget& budget,
                         BridgePumpResult* result) noexcept;
    Status PullOutbound(const BridgePumpBudget& budget,
                        BridgePumpResult* result) noexcept;
    Status PrepareSchema(EncodedOutboundFrame* outbound,
                         bool* control_pending) noexcept;
    Status SendData(EncodedOutboundFrame outbound, uint64_t now_ns,
                    const BridgePumpBudget& budget,
                    BridgePumpResult* result,
                    bool* ownership_transferred) noexcept;
    PendingReliable* FindPendingReliable(const SourceIdentity& source,
                                         uint64_t sequence) noexcept;
    void RemoveRetiredReliable() noexcept;

    BridgePipelineOptions options_;
    std::shared_ptr<transport::TransportDriver> driver_;
    transport::ConnectionId connection_id_;
    BridgeEgressPort* egress_;
    BridgeIngressPort* ingress_;
    SchemaNegotiator* schema_negotiator_;
    std::unique_ptr<DedupWindow> dedup_;
    std::unique_ptr<RetransmitWindow> retransmit_;
    std::deque<std::vector<std::byte>> control_queue_;
    size_t control_bytes_ = 0;
    std::vector<AckPayload> pending_acks_;
    std::vector<WireFrame> pending_schema_controls_;
    std::vector<PendingInbound> pending_inbound_;
    std::vector<PendingInbound> staged_ready_;
    size_t pending_inbound_bytes_ = 0;
    size_t processing_inbound_wire_bytes_ = 0;
    std::vector<PendingReliable> pending_reliable_;
    std::vector<LocalSchemaBinding> local_schema_bindings_;
    std::vector<Attempt> attempts_;
    bool hello_sent_ = false;
    bool hello_received_ = false;
    bool session_ready_ = false;
    bool peer_dedup_state_lost_ = false;
    bool resend_pending_ = false;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_BRIDGE_PIPELINE_H_
