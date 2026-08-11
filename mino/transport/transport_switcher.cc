// Copyright 2026 The Mino Authors

#include "mino/transport/transport_switcher.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <new>
#include <unordered_map>
#include <utility>

namespace mino::transport {
namespace {

Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted,
                         "transport switcher allocation failed");
}

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsUsableNode(const registry::NodeMetadata& node) noexcept {
    return node.lease_state == registry::NodeLeaseState::kActive &&
           node.liveness == ProcessIdentityLiveness::kAlive &&
           node.health != registry::NodeHealth::kUnavailable;
}

bool ReliabilitySatisfies(registry::Reliability requested,
                          TransportReliability available) noexcept {
    if (requested == registry::Reliability::kBestEffort) {
        return true;
    }
    return requested == registry::Reliability::kReliableOrdered &&
           available == TransportReliability::kReliable;
}

struct DriverCandidate {
    EndpointDescriptor endpoint;
    uint64_t node_config_version = 0;
    ProcessIdentity process_identity;
    uint64_t lease_epoch = 0;
    DriverRegistration registration;
};

struct TargetCandidate {
    NodeId target_node;
    std::shared_ptr<const LocalPublicationBinding> local_binding;
    std::vector<DriverCandidate> remote;
};

struct CachedPlan {
    RouteStamp stamp;
    std::shared_ptr<const registry::TopicSnapshot> topic;
    std::vector<TargetCandidate> targets;
};

}  // namespace

class TransportSwitcher::Impl final {
public:
    Impl(NodeId local_node, const registry::Coordinator* coordinator,
         std::shared_ptr<const RouteAccessValidator> access_validator,
         std::shared_ptr<const SchemaRouteValidator> schema_validator,
         std::shared_ptr<const LocalRouteProvider> local_provider,
         size_t max_cached_topics, size_t max_drivers)
        : local_node_(local_node),
          coordinator_(coordinator),
          access_validator_(std::move(access_validator)),
          schema_validator_(std::move(schema_validator)),
          local_provider_(std::move(local_provider)),
          max_cached_topics_(max_cached_topics),
          max_drivers_(max_drivers) {}

    Status RegisterDriver(DriverRegistration registration) {
        try {
            if (registration.driver_id == 0 || registration.generation == 0 ||
                registration.driver == nullptr ||
                registration.endpoint_matcher == nullptr) {
                return Invalid("driver registration is incomplete");
            }
            MINO_RETURN_IF_ERROR(ValidateTransportCapabilities(
                registration.driver->capabilities()));

            std::lock_guard lock(mutex_);
            if (drivers_.contains(registration.driver_id)) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "driver ID is already registered");
            }
            if (drivers_.size() >= max_drivers_ ||
                driver_registry_version_ ==
                    std::numeric_limits<uint64_t>::max()) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "transport switcher driver limit reached");
            }
            drivers_.emplace(registration.driver_id, std::move(registration));
            AdvanceDriverVersionLocked();
            plans_.clear();
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return AllocationFailure();
        }
    }

    Status UnregisterDriver(uint64_t driver_id, uint64_t generation) {
        if (driver_id == 0 || generation == 0) {
            return Invalid("driver unregister token is incomplete");
        }
        std::lock_guard lock(mutex_);
        const auto found = drivers_.find(driver_id);
        if (found == drivers_.end() || found->second.generation != generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "driver token does not exactly match");
        }
        if (driver_registry_version_ ==
            std::numeric_limits<uint64_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "driver registry version exhausted");
        }
        drivers_.erase(found);
        AdvanceDriverVersionLocked();
        plans_.clear();
        return Status::Ok();
    }

    Status RefreshTopic(TopicId topic_id) {
        uint64_t refresh_id = 0;
        try {
            if (topic_id.value == 0) {
                return Invalid("route refresh topic ID is zero");
            }

            {
                std::lock_guard lock(mutex_);
                if (next_refresh_id_ == std::numeric_limits<uint64_t>::max()) {
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "route refresh generation exhausted");
                }
                refresh_id = ++next_refresh_id_;
                latest_refresh_[topic_id] = refresh_id;
            }

            auto result = BuildPlan(topic_id);
            if (!result.ok()) {
                InvalidateFailedRefresh(topic_id, refresh_id);
                return result.status();
            }
            std::shared_ptr<const CachedPlan> plan = std::move(*result);

            std::lock_guard lock(mutex_);
            const auto latest = latest_refresh_.find(topic_id);
            if (latest == latest_refresh_.end() || latest->second != refresh_id) {
                return Status::Error(StatusCode::kUnavailable,
                                     "route refresh was superseded");
            }
            if (plan->stamp.driver_registry_version != driver_registry_version_) {
                plans_.erase(topic_id);
                return Status::Error(StatusCode::kUnavailable,
                                     "driver registry changed during refresh");
            }
            if (!plans_.contains(topic_id) &&
                plans_.size() >= max_cached_topics_) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "transport switcher topic cache is full");
            }
            plans_[topic_id] = std::move(plan);
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            if (refresh_id != 0) {
                InvalidateFailedRefresh(topic_id, refresh_id);
            }
            return AllocationFailure();
        }
    }

    Status InvalidateTopic(TopicId topic_id) {
        if (topic_id.value == 0) {
            return Invalid("route invalidation topic ID is zero");
        }
        std::lock_guard lock(mutex_);
        plans_.erase(topic_id);
        latest_refresh_.erase(topic_id);
        return Status::Ok();
    }

    Result<std::shared_ptr<const RouteHandle>> Resolve(
        const RouteRequest& request) const {
        try {
            if (request.topic_id.value == 0 || request.payload_size == 0) {
                return Invalid("route request topic and payload must be nonzero");
            }
            if (request.payload_size > kMaxPayloadBytes) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "route payload exceeds absolute bound");
            }

            std::shared_ptr<const CachedPlan> plan;
            {
                std::lock_guard lock(mutex_);
                const auto found = plans_.find(request.topic_id);
                if (found == plans_.end()) {
                    return Status::Error(StatusCode::kUnavailable,
                                         "route cache requires refresh");
                }
                plan = found->second;
                if (plan->stamp.driver_registry_version !=
                    driver_registry_version_) {
                    return Status::Error(StatusCode::kUnavailable,
                                         "driver registry route stamp is stale");
                }
            }

            if (access_validator_->version() !=
                    plan->stamp.acl_validator_version ||
                schema_validator_->version() !=
                    plan->stamp.schema_validator_version ||
                local_provider_->version() !=
                    plan->stamp.local_provider_version) {
                return Status::Error(StatusCode::kUnavailable,
                                     "route dependency version changed; refresh required");
            }
            const registry::TopicMetadata& topic = plan->topic->metadata;
            if (request.delivery != topic.delivery) {
                return Invalid("route delivery policy differs from topic policy");
            }
            if (!registry::SchemaIdentityEqual(request.publisher_schema,
                                               topic.schema)) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "publisher schema differs from cached topic schema");
            }

            auto handle = std::make_shared<RouteHandle>();
            handle->stamp_ = plan->stamp;
            handle->delivery_ = request.delivery;
            handle->payload_size_ = request.payload_size;
            handle->priority_ = request.priority;
            handle->targets_.reserve(plan->targets.size());

            for (const TargetCandidate& target : plan->targets) {
                TargetRoute selected{
                    .target_node = target.target_node,
                    .transport = LocalTargetRoute{},
                };
                if (target.local_binding != nullptr) {
                    selected.transport = LocalTargetRoute{
                        .binding = target.local_binding,
                    };
                    handle->targets_.push_back(std::move(selected));
                    continue;
                }

                const DriverCandidate* best = nullptr;
                TransportCapabilities best_capabilities{};
                HealthState best_health = HealthState::kUnavailable;
                bool payload_rejected = false;
                bool reliability_rejected = false;
                for (const DriverCandidate& candidate : target.remote) {
                    const std::shared_ptr<TransportDriver>& driver =
                        candidate.registration.driver;
                    if (driver->state() != DriverState::kRunning) {
                        continue;
                    }
                    const HealthState health = driver->health();
                    if (health != HealthState::kHealthy &&
                        health != HealthState::kDegraded) {
                        continue;
                    }
                    const TransportCapabilities capabilities =
                        driver->capabilities();
                    if (!ValidateTransportCapabilities(capabilities).ok() ||
                        capabilities.kind != candidate.endpoint.kind()) {
                        continue;
                    }
                    if (!ReliabilitySatisfies(topic.delivery.reliability,
                                              capabilities.reliability)) {
                        reliability_rejected = true;
                        continue;
                    }
                    if (capabilities.max_frame_size != 0 &&
                        request.payload_size > capabilities.max_frame_size) {
                        payload_rejected = true;
                        continue;
                    }
                    const bool healthier =
                        best == nullptr ||
                        static_cast<uint8_t>(health) <
                            static_cast<uint8_t>(best_health);
                    const bool same_health_higher_priority =
                        best != nullptr && health == best_health &&
                        candidate.registration.selection_priority >
                            best->registration.selection_priority;
                    const bool deterministic_tie_break =
                        best != nullptr && health == best_health &&
                        candidate.registration.selection_priority ==
                            best->registration.selection_priority &&
                        candidate.registration.driver_id <
                            best->registration.driver_id;
                    if (healthier || same_health_higher_priority ||
                        deterministic_tie_break) {
                        best = &candidate;
                        best_capabilities = capabilities;
                        best_health = health;
                    }
                }
                if (best == nullptr) {
                    if (payload_rejected) {
                        return Status::Error(
                            StatusCode::kResourceExhausted,
                            "no route driver accepts the payload size");
                    }
                    if (reliability_rejected) {
                        return Status::Error(
                            StatusCode::kUnsupported,
                            "no route driver satisfies topic reliability");
                    }
                    return Status::Error(StatusCode::kUnavailable,
                                         "no healthy running route driver");
                }
                selected.transport = RemoteTargetRoute{
                    .endpoint = best->endpoint,
                    .node_config_version = best->node_config_version,
                    .process_identity = best->process_identity,
                    .lease_epoch = best->lease_epoch,
                    .driver_id = best->registration.driver_id,
                    .driver_generation = best->registration.generation,
                    .capabilities = best_capabilities,
                    .driver = best->registration.driver,
                };
                handle->targets_.push_back(std::move(selected));
            }
            return std::shared_ptr<const RouteHandle>(std::move(handle));
        } catch (const std::bad_alloc&) {
            return AllocationFailure();
        }
    }

private:
    Result<std::shared_ptr<const CachedPlan>> BuildPlan(TopicId topic_id) const {
        MINO_ASSIGN_OR_RETURN(auto routing,
                              coordinator_->GetRoutingSnapshot(topic_id));
        MINO_ASSIGN_OR_RETURN(auto nodes, coordinator_->NodeSnapshot());

        if (routing->topic->metadata.state != registry::TopicState::kActive) {
            return Status::Error(StatusCode::kUnavailable,
                                 "only an active topic can build new routes");
        }
        if (routing->routes->topic_id != topic_id ||
            routing->routes->policy !=
                routing->topic->metadata.route_policy ||
            (routing->routes->policy == registry::RoutePolicy::kStatic &&
             routing->routes->version !=
                 routing->topic->metadata.route_set_version)) {
            return Status::Error(StatusCode::kCorruption,
                                 "registry returned an inconsistent routing snapshot");
        }

        std::vector<DriverRegistration> drivers;
        uint64_t driver_version = 0;
        {
            std::lock_guard lock(mutex_);
            drivers.reserve(drivers_.size());
            for (const auto& [driver_id, registration] : drivers_) {
                (void)driver_id;
                drivers.push_back(registration);
            }
            driver_version = driver_registry_version_;
        }

        const uint64_t acl_version = access_validator_->version();
        const uint64_t schema_version = schema_validator_->version();
        const uint64_t local_version = local_provider_->version();

        auto plan = std::make_shared<CachedPlan>();
        plan->stamp = RouteStamp{
            .topic_id = topic_id,
            .policy = routing->routes->policy,
            .topic_config_version =
                routing->topic->metadata.config_version,
            .route_version = routing->routes->version,
            .node_registry_version = nodes->version,
            .driver_registry_version = driver_version,
            .acl_validator_version = acl_version,
            .schema_validator_version = schema_version,
            .local_provider_version = local_version,
        };
        plan->topic = routing->topic;
        plan->targets.reserve(routing->routes->routes.size());
        const auto local_node = std::lower_bound(
            nodes->nodes.begin(), nodes->nodes.end(), local_node_,
            [](const registry::NodeMetadata& candidate, NodeId target) {
                return candidate.node_id < target;
            });
        if (local_node == nodes->nodes.end() ||
            local_node->node_id != local_node_) {
            return Status::Error(StatusCode::kNotFound,
                                 "local source node is not registered");
        }

        for (const registry::StaticRouteEntry& route :
             routing->routes->routes) {
            const auto node = std::lower_bound(
                nodes->nodes.begin(), nodes->nodes.end(), route.target_node,
                [](const registry::NodeMetadata& candidate, NodeId target) {
                    return candidate.node_id < target;
                });
            if (node == nodes->nodes.end() ||
                node->node_id != route.target_node) {
                return Status::Error(StatusCode::kNotFound,
                                     "route target node is not registered");
            }
            if (!IsUsableNode(*node)) {
                return Status::Error(StatusCode::kUnavailable,
                                     "route target node is unavailable");
            }
            // The metadata ACL is mandatory and cannot be bypassed by a custom
            // deployment validator. The injected validator may only add policy.
            MINO_RETURN_IF_ERROR(registry::ValidateTopicPermission(
                routing->topic->metadata, local_node->security_domain_id,
                local_node_, registry::TopicPermission::kPublish));
            MINO_RETURN_IF_ERROR(registry::ValidateTopicPermission(
                routing->topic->metadata, node->security_domain_id,
                route.target_node, registry::TopicPermission::kSubscribe));
            MINO_RETURN_IF_ERROR(access_validator_->Validate(
                routing->topic->metadata, local_node_, route.target_node));
            MINO_RETURN_IF_ERROR(schema_validator_->Validate(
                routing->topic->metadata, route.target_node,
                routing->topic->metadata.schema));

            TargetCandidate target{
                .target_node = route.target_node,
                .local_binding = nullptr,
                .remote = {},
            };
            if (route.target_node == local_node_) {
                MINO_ASSIGN_OR_RETURN(
                    target.local_binding,
                    local_provider_->Resolve(routing->topic->metadata));
                if (target.local_binding == nullptr) {
                    return Status::Error(StatusCode::kUnavailable,
                                         "local route provider returned no binding");
                }
            } else {
                for (const EndpointDescriptor& endpoint : node->endpoints) {
                    if (route.preferred_transport.has_value() &&
                        endpoint.kind() != *route.preferred_transport) {
                        continue;
                    }
                    for (const DriverRegistration& registration : drivers) {
                        if (!registration.endpoint_matcher->Supports(endpoint)) {
                            continue;
                        }
                        const TransportCapabilities capabilities =
                            registration.driver->capabilities();
                        if (!ValidateTransportCapabilities(capabilities).ok() ||
                            capabilities.kind != endpoint.kind()) {
                            continue;
                        }
                        target.remote.push_back(DriverCandidate{
                            .endpoint = endpoint,
                            .node_config_version = node->config_version,
                            .process_identity = node->process_identity,
                            .lease_epoch = node->lease_epoch,
                            .registration = registration,
                        });
                    }
                }
                if (target.remote.empty()) {
                    return Status::Error(
                        StatusCode::kUnsupported,
                        route.preferred_transport.has_value()
                            ? "preferred transport has no matching driver"
                            : "target node has no supported endpoint");
                }
            }
            plan->targets.push_back(std::move(target));
        }

        if (access_validator_->version() != acl_version ||
            schema_validator_->version() != schema_version ||
            local_provider_->version() != local_version) {
            return Status::Error(StatusCode::kUnavailable,
                                 "route dependency changed during refresh");
        }
        return std::shared_ptr<const CachedPlan>(std::move(plan));
    }

    void InvalidateFailedRefresh(TopicId topic_id, uint64_t refresh_id) {
        std::lock_guard lock(mutex_);
        const auto latest = latest_refresh_.find(topic_id);
        if (latest != latest_refresh_.end() && latest->second == refresh_id) {
            plans_.erase(topic_id);
        }
    }

    void AdvanceDriverVersionLocked() noexcept { ++driver_registry_version_; }

    NodeId local_node_;
    const registry::Coordinator* coordinator_;
    std::shared_ptr<const RouteAccessValidator> access_validator_;
    std::shared_ptr<const SchemaRouteValidator> schema_validator_;
    std::shared_ptr<const LocalRouteProvider> local_provider_;
    size_t max_cached_topics_;
    size_t max_drivers_;

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, DriverRegistration> drivers_;
    std::unordered_map<TopicId, std::shared_ptr<const CachedPlan>> plans_;
    std::unordered_map<TopicId, uint64_t> latest_refresh_;
    uint64_t driver_registry_version_ = 1;
    uint64_t next_refresh_id_ = 0;
};

Result<std::unique_ptr<TransportSwitcher>> TransportSwitcher::Create(
    NodeId local_node, const registry::Coordinator* coordinator,
    std::shared_ptr<const RouteAccessValidator> access_validator,
    std::shared_ptr<const SchemaRouteValidator> schema_validator,
    std::shared_ptr<const LocalRouteProvider> local_provider,
    size_t max_cached_topics, size_t max_drivers) {
    try {
        if (local_node.value == 0 || coordinator == nullptr ||
            access_validator == nullptr || schema_validator == nullptr ||
            local_provider == nullptr) {
            return Invalid("transport switcher dependencies are incomplete");
        }
        if (max_cached_topics == 0 ||
            max_cached_topics > kMaxSwitcherCachedTopics || max_drivers == 0 ||
            max_drivers > kMaxSwitcherDrivers) {
            return Invalid("transport switcher limits are invalid");
        }
        auto impl = std::make_unique<Impl>(
            local_node, coordinator, std::move(access_validator),
            std::move(schema_validator), std::move(local_provider),
            max_cached_topics, max_drivers);
        return std::unique_ptr<TransportSwitcher>(
            new TransportSwitcher(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

TransportSwitcher::TransportSwitcher(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

TransportSwitcher::~TransportSwitcher() = default;

Status TransportSwitcher::RegisterDriver(DriverRegistration registration) {
    return impl_->RegisterDriver(std::move(registration));
}

Status TransportSwitcher::UnregisterDriver(uint64_t driver_id,
                                            uint64_t generation) {
    return impl_->UnregisterDriver(driver_id, generation);
}

Status TransportSwitcher::RefreshTopic(TopicId topic_id) {
    return impl_->RefreshTopic(topic_id);
}

Status TransportSwitcher::InvalidateTopic(TopicId topic_id) {
    return impl_->InvalidateTopic(topic_id);
}

Result<std::shared_ptr<const RouteHandle>> TransportSwitcher::Resolve(
    const RouteRequest& request) const {
    return impl_->Resolve(request);
}

}  // namespace mino::transport
