// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/bus.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace mino {
namespace {

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

schema::SchemaIdentity TestSchema(uint64_t short_id = 77) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(short_id);
    digest[31] = std::byte{0x5a};
    return schema::SchemaIdentity(short_id, digest, 1, 1);
}

ProcessIdentity TestProcessIdentity(NodeId node) {
    return ProcessIdentity{
        .node_id = node.value,
        .process_id = 1001,
        .process_epoch = 9,
        .start_time_ns = 123456,
    };
}

class FakeAccessValidator final : public transport::RouteAccessValidator {
public:
    uint64_t version() const noexcept override { return 1; }
    Status Validate(const registry::TopicMetadata&, NodeId source,
                    NodeId target) const override {
        return source.value != 0 && target.value != 0
                   ? Status::Ok()
                   : Status::Error(StatusCode::kInvalidArgument);
    }
};

class FakeSchemaValidator final : public transport::SchemaRouteValidator {
public:
    uint64_t version() const noexcept override { return 1; }
    Status Validate(const registry::TopicMetadata& topic, NodeId,
                    const schema::SchemaIdentity& publisher_schema) const override {
        return registry::SchemaIdentityEqual(topic.schema, publisher_schema)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kSchemaMismatch);
    }
};

class FakePublicationBinding final
    : public transport::LocalPublicationBinding {};

class FakeRouteProvider final : public transport::LocalRouteProvider {
public:
    FakeRouteProvider()
        : binding_(std::make_shared<const FakePublicationBinding>()) {}

    uint64_t version() const noexcept override { return version_; }

    Result<std::shared_ptr<const transport::LocalPublicationBinding>> Resolve(
        const registry::TopicMetadata&) const override {
        if (!available_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "route binding unavailable");
        }
        return binding_;
    }

    void SetAvailable(bool available) noexcept {
        available_ = available;
        ++version_;
    }

private:
    std::shared_ptr<const transport::LocalPublicationBinding> binding_;
    uint64_t version_ = 1;
    bool available_ = true;
};

class FakeParticipantIds final : public ParticipantIdAllocator {
public:
    Result<PublisherParticipantIdentity> AllocatePublisher() override {
        ++publisher_allocations;
        return PublisherParticipantIdentity{
            .publisher_id = PublisherId{next_publisher++},
            .generation = next_generation++,
        };
    }

    Result<SubscriberParticipantIdentity> AllocateSubscriber() override {
        ++subscriber_allocations;
        return SubscriberParticipantIdentity{
            .subscriber_id = SubscriberId{next_subscriber++},
            .generation = next_generation++,
        };
    }

    uint64_t next_publisher = 100;
    uint32_t next_subscriber = 200;
    uint64_t next_generation = 300;
    size_t publisher_allocations = 0;
    size_t subscriber_allocations = 0;
};

class FakeLocalPublisher final : public BusLocalPublisherEndpoint {
public:
    FakeLocalPublisher(registry::PublisherRegistration registration,
                       std::shared_ptr<std::vector<std::string>> events)
        : registration_(registration), events_(std::move(events)) {}

    Result<LocalPublication> Publish(std::span<const std::byte> payload,
                                     uint8_t priority) override {
        events_->push_back("local-publish");
        ++calls;
        last_payload.assign(payload.begin(), payload.end());
        last_priority = priority;
        return LocalPublication{
            .source = {
                .node_id = registration_.owner.node_id.value,
                .publisher_id = registration_.publisher_id.value,
                .publisher_epoch = registration_.generation,
            },
            .sequence_num = next_sequence++,
            .timestamp_ns = 999,
            .message_type = 42,
        };
    }

    size_t calls = 0;
    std::vector<std::byte> last_payload;
    uint8_t last_priority = 0;
    uint64_t next_sequence = 1;

private:
    registry::PublisherRegistration registration_;
    std::shared_ptr<std::vector<std::string>> events_;
};

class FakeLocalSubscriber final : public BusLocalSubscriberEndpoint {
public:
    Result<CanonicalMessage> TryPoll() override {
        ++calls;
        if (messages.empty()) {
            return Status::Error(StatusCode::kWouldBlock);
        }
        CanonicalMessage message = std::move(messages.front());
        messages.pop_front();
        return message;
    }

    size_t calls = 0;
    std::deque<CanonicalMessage> messages;
};

class FakeEndpointProvider final : public BusLocalEndpointProvider {
public:
    FakeEndpointProvider(
        registry::Coordinator* coordinator,
        std::shared_ptr<std::vector<std::string>> events)
        : coordinator_(coordinator),
          events_(std::move(events)),
          binding_(std::make_shared<const FakePublicationBinding>()) {}

    Result<BusLocalPublisherResources> OpenPublisher(
        const registry::TopicMetadata& topic,
        const registry::PublisherRegistration& registration) override {
        auto snapshot = coordinator_->GetTopic(topic.topic_id);
        observed_publishers = snapshot.ok() ? snapshot.value()->usage.publishers : 0;
        if (fail_publish_open) {
            return Status::Error(StatusCode::kUnavailable,
                                 "publisher open failed");
        }
        auto endpoint =
            std::make_shared<FakeLocalPublisher>(registration, events_);
        weak_last_publisher = endpoint;
        last_publisher = retain_last_endpoints ? endpoint : nullptr;
        return BusLocalPublisherResources{
            .binding = Binding(topic),
            .endpoint = std::move(endpoint),
        };
    }

    Result<BusLocalSubscriberResources> OpenSubscriber(
        const registry::TopicMetadata& topic,
        const registry::SubscriberRegistration&) override {
        auto snapshot = coordinator_->GetTopic(topic.topic_id);
        observed_subscribers =
            snapshot.ok() ? snapshot.value()->usage.subscribers : 0;
        if (fail_subscriber_open) {
            return Status::Error(StatusCode::kUnavailable,
                                 "subscriber open failed");
        }
        auto endpoint = std::make_shared<FakeLocalSubscriber>();
        weak_last_subscriber = endpoint;
        last_subscriber = retain_last_endpoints ? endpoint : nullptr;
        return BusLocalSubscriberResources{
            .binding = Binding(topic),
            .endpoint = std::move(endpoint),
        };
    }

    std::weak_ptr<const transport::LocalPublicationBinding> WeakBinding() const {
        return binding_;
    }

    void DropBinding() { binding_.reset(); }

    bool fail_publish_open = false;
    bool fail_subscriber_open = false;
    bool stale_binding = false;
    bool retain_last_endpoints = true;
    bool release_binding_after_open = false;
    uint32_t observed_publishers = 0;
    uint32_t observed_subscribers = 0;
    std::shared_ptr<FakeLocalPublisher> last_publisher;
    std::shared_ptr<FakeLocalSubscriber> last_subscriber;
    std::weak_ptr<FakeLocalPublisher> weak_last_publisher;
    std::weak_ptr<FakeLocalSubscriber> weak_last_subscriber;
    std::weak_ptr<const transport::LocalPublicationBinding> weak_last_binding;

private:
    BusLocalResourceBinding Binding(const registry::TopicMetadata& topic) {
        std::shared_ptr<const transport::LocalPublicationBinding> publication =
            binding_;
        weak_last_binding = publication;
        if (release_binding_after_open) {
            binding_.reset();
        }
        return BusLocalResourceBinding{
            .topic_id = topic.topic_id,
            .region_version = topic.region_version,
            .channel_version =
                stale_binding ? topic.channel_version + 1 : topic.channel_version,
            .acl_version = topic.acl_version,
            .publication = std::move(publication),
        };
    }

    registry::Coordinator* coordinator_;
    std::shared_ptr<std::vector<std::string>> events_;
    std::shared_ptr<const transport::LocalPublicationBinding> binding_;
};

class FakeBridgeDispatcher final : public BridgeDispatcher {
public:
    explicit FakeBridgeDispatcher(
        std::shared_ptr<std::vector<std::string>> events)
        : events_(std::move(events)) {}

    Status Dispatch(const BridgeDispatchRequest& request) override {
        events_->push_back("dispatch");
        ++calls;
        topic_id = request.topic_id;
        schema = request.schema;
        publication = request.publication;
        priority = request.priority;
        payload.assign(request.canonical_payload.begin(),
                       request.canonical_payload.end());
        route = request.route;
        if (throw_exception) {
            throw std::runtime_error("dispatch failed");
        }
        return dispatch_status;
    }

    size_t calls = 0;
    TopicId topic_id;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    LocalPublication publication;
    uint8_t priority = 0;
    std::vector<std::byte> payload;
    std::shared_ptr<const transport::RouteHandle> route;
    Status dispatch_status = Status::Ok();
    bool throw_exception = false;

private:
    std::shared_ptr<std::vector<std::string>> events_;
};

class BusTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto coordinator = registry::Coordinator::CreateForTesting();
        ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
        coordinator_ = std::shared_ptr<registry::Coordinator>(
            std::move(*coordinator));

        const uint64_t now = NowNs();
        auto endpoint = transport::EndpointDescriptor::SharedFabric(1, 1);
        ASSERT_TRUE(endpoint.ok()) << endpoint.status().ToString();
        registry::NodeRegistration node{
            .node_id = local_node_,
            .process_identity = TestProcessIdentity(local_node_),
            .endpoints = {*endpoint},
            .trust_domain = "bus-test",
            .health = registry::NodeHealth::kHealthy,
            .lease_epoch = 5,
            .lease_duration_ns = registry::kMaxLeaseDurationNs,
            .config_version = 1,
        };
        auto registered = coordinator_->RegisterNode(node, now);
        ASSERT_TRUE(registered.ok()) << registered.status().ToString();
        owner_ = registry::NodeLeaseOwner{
            .node_id = local_node_,
            .process_identity = node.process_identity,
            .lease_epoch = node.lease_epoch,
        };

        access_ = std::make_shared<FakeAccessValidator>();
        schema_validator_ = std::make_shared<FakeSchemaValidator>();
        route_provider_ = std::make_shared<FakeRouteProvider>();
        auto switcher = transport::TransportSwitcher::Create(
            local_node_, coordinator_.get(), access_, schema_validator_,
            route_provider_);
        ASSERT_TRUE(switcher.ok()) << switcher.status().ToString();
        switcher_ = std::shared_ptr<transport::TransportSwitcher>(
            std::move(*switcher));

        events_ = std::make_shared<std::vector<std::string>>();
        ids_ = std::make_shared<FakeParticipantIds>();
        endpoints_ =
            std::make_shared<FakeEndpointProvider>(coordinator_.get(), events_);
        dispatcher_ = std::make_shared<FakeBridgeDispatcher>(events_);
        fault_injector_ = std::make_shared<BusLifecycleFaultInjector>();
        auto bus = Bus::Create(owner_, coordinator_, switcher_, ids_, endpoints_,
                               dispatcher_, fault_injector_);
        ASSERT_TRUE(bus.ok()) << bus.status().ToString();
        bus_ = std::move(*bus);
    }

    TopicId CreateTopic(bool activate = true) {
        registry::TopicMetadata candidate{
            .topic_id = {},
            .name = "bus/topic/" + std::to_string(next_topic_name_++),
            .channel_kind = registry::ChannelKind::kBroadcast,
            .delivery = {
                .reliability = registry::Reliability::kBestEffort,
                .allow_drop = false,
            },
            .queue_full_policy = QueueFullPolicy::kBlock,
            .schema = TestSchema(),
            .accepted_schemas = {},
            .route_policy = registry::RoutePolicy::kStatic,
            .static_routes = {{
                .target_node = local_node_,
                .preferred_transport = std::nullopt,
            }},
            .route_set_version = 0,
            .capacity = 64,
            .max_publishers = 8,
            .max_subscribers = 8,
            .partition_count = 1,
            .record_topology =
                registry::RecordBackpressureTopology::kIsolated,
            .region_version = 11,
            .channel_version = 12,
            .acl_version = 13,
            .config_version = 0,
            .state = registry::TopicState::kCreating,
        };
        auto created = coordinator_->CreateTopic(std::move(candidate));
        EXPECT_TRUE(created.ok()) << created.status().ToString();
        if (!created.ok()) {
            return {};
        }
        const registry::TopicMetadata& topic = (*created)->metadata;
        if (activate) {
            registry::ActivationReadinessProof proof{
                .topic_id = topic.topic_id,
                .config_version = topic.config_version,
                .schema = topic.schema,
                .region_version = topic.region_version,
                .channel_version = topic.channel_version,
                .acl_version = topic.acl_version,
                .schema_ready = true,
                .region_ready = true,
                .channel_ready = true,
                .acl_ready = true,
            };
            EXPECT_TRUE(coordinator_->ActivateTopic(topic.topic_id, proof).ok());
        }
        return topic.topic_id;
    }

    registry::TopicUsageCounts Usage(TopicId topic_id) {
        auto topic = coordinator_->GetTopic(topic_id);
        EXPECT_TRUE(topic.ok()) << topic.status().ToString();
        return topic.ok() ? (*topic)->usage : registry::TopicUsageCounts{};
    }

    std::string TopicName(TopicId topic_id) {
        auto topic = coordinator_->GetTopic(topic_id);
        EXPECT_TRUE(topic.ok()) << topic.status().ToString();
        return topic.ok() ? (*topic)->metadata.name : std::string{};
    }

    const NodeId local_node_{1};
    registry::NodeLeaseOwner owner_;
    std::shared_ptr<registry::Coordinator> coordinator_;
    std::shared_ptr<FakeAccessValidator> access_;
    std::shared_ptr<FakeSchemaValidator> schema_validator_;
    std::shared_ptr<FakeRouteProvider> route_provider_;
    std::shared_ptr<transport::TransportSwitcher> switcher_;
    std::shared_ptr<std::vector<std::string>> events_;
    std::shared_ptr<FakeParticipantIds> ids_;
    std::shared_ptr<FakeEndpointProvider> endpoints_;
    std::shared_ptr<FakeBridgeDispatcher> dispatcher_;
    std::shared_ptr<BusLifecycleFaultInjector> fault_injector_;
    std::unique_ptr<Bus> bus_;
    uint64_t next_topic_name_ = 1;
};

TEST_F(BusTest, CreatesPublisherAndSubscriberByNameAndId) {
    const TopicId first = CreateTopic();
    const TopicId second = CreateTopic();

    {
        auto publisher = bus_->CreatePublisher(TopicName(first), TestSchema());
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
        EXPECT_EQ(publisher->topic_id(), first);
        EXPECT_EQ(Usage(first).publishers, 1u);

        auto subscriber = bus_->CreateSubscriber(second, TestSchema());
        ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
        EXPECT_EQ(subscriber->topic_id(), second);
        EXPECT_EQ(Usage(second).subscribers, 1u);
    }

    auto publisher = bus_->CreatePublisher(first, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    auto subscriber =
        bus_->CreateSubscriber(TopicName(second), TestSchema());
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
}

TEST_F(BusTest, RejectsIncompleteMismatchedAndNonActiveTopics) {
    const TopicId active = CreateTopic();
    const TopicId creating = CreateTopic(false);

    auto incomplete = bus_->CreatePublisher(
        active, schema::SchemaIdentity(0, {}, 0, 0));
    ASSERT_FALSE(incomplete.ok());
    EXPECT_EQ(incomplete.status().code(), StatusCode::kInvalidArgument);

    auto mismatch = bus_->CreateSubscriber(active, TestSchema(88));
    ASSERT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.status().code(), StatusCode::kSchemaMismatch);

    auto wrong_state = bus_->CreatePublisher(creating, TestSchema());
    ASSERT_FALSE(wrong_state.ok());
    EXPECT_EQ(wrong_state.status().code(), StatusCode::kUnavailable);

    ASSERT_TRUE(coordinator_->DrainTopic(active).ok());
    auto draining = bus_->CreateSubscriber(active, TestSchema());
    ASSERT_FALSE(draining.ok());
    EXPECT_EQ(draining.status().code(), StatusCode::kUnavailable);
}

TEST_F(BusTest, ExplicitlyAcceptedPreviousSchemaIsSubscriberOnly) {
    const TopicId topic = CreateTopic();
    auto current = coordinator_->GetTopic(topic);
    ASSERT_TRUE(current.ok());
    registry::TopicMetadata updated = (*current)->metadata;
    updated.accepted_schemas = {TestSchema(88)};
    const uint64_t expected = updated.config_version;
    ++updated.config_version;
    ASSERT_TRUE(coordinator_->UpdateTopic(updated, expected).ok());

    auto subscriber = bus_->CreateSubscriber(topic, TestSchema(88));
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
    auto publisher = bus_->CreatePublisher(topic, TestSchema(88));
    ASSERT_FALSE(publisher.ok());
    EXPECT_EQ(publisher.status().code(), StatusCode::kSchemaMismatch);
}

TEST_F(BusTest, ReservesDeferredCapacityBeforeRegistration) {
    const TopicId topic = CreateTopic();

    fault_injector_->FailPublisherSlotReservationAttempts(1);
    auto publisher_failure = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_FALSE(publisher_failure.ok());
    EXPECT_EQ(publisher_failure.status().code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(endpoints_->observed_publishers, 0u);
    EXPECT_EQ(Usage(topic).publishers, 0u);

    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    EXPECT_EQ(Usage(topic).publishers, 1u);
    EXPECT_TRUE(publisher->Close().ok());

    fault_injector_->FailSubscriberSlotReservationAttempts(1);
    auto subscriber_failure = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_FALSE(subscriber_failure.ok());
    EXPECT_EQ(subscriber_failure.status().code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(endpoints_->observed_subscribers, 0u);
    EXPECT_EQ(Usage(topic).subscribers, 0u);

    auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
    EXPECT_EQ(Usage(topic).subscribers, 1u);
    EXPECT_TRUE(subscriber->Close().ok());
}

TEST_F(BusTest, RegisterFailureReleasesReservedDeferredSlot) {
    const TopicId topic = CreateTopic();
    std::vector<BusPublisher> publishers;
    publishers.reserve(8);
    for (size_t index = 0; index < 8; ++index) {
        auto publisher = bus_->CreatePublisher(topic, TestSchema());
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
        publishers.push_back(std::move(*publisher));
    }
    EXPECT_EQ(Usage(topic).publishers, 8u);

    auto over_limit = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_FALSE(over_limit.ok());
    EXPECT_EQ(over_limit.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(Usage(topic).publishers, 8u);

    EXPECT_TRUE(publishers.back().Close().ok());
    publishers.pop_back();
    auto replacement = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(replacement.ok()) << replacement.status().ToString();
    EXPECT_TRUE(replacement->Close().ok());
    for (BusPublisher& publisher : publishers) {
        EXPECT_TRUE(publisher.Close().ok());
    }
    EXPECT_EQ(Usage(topic).publishers, 0u);
}

TEST_F(BusTest, RollsBackExactRegistrationAfterEveryLaterFailure) {
    const TopicId topic = CreateTopic();

    endpoints_->fail_publish_open = true;
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_FALSE(publisher.ok());
    EXPECT_EQ(endpoints_->observed_publishers, 1u);
    EXPECT_EQ(Usage(topic).publishers, 0u);

    endpoints_->fail_publish_open = false;
    endpoints_->stale_binding = true;
    auto stale = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_FALSE(stale.ok());
    EXPECT_EQ(stale.status().code(), StatusCode::kUnavailable);
    EXPECT_EQ(Usage(topic).publishers, 0u);

    endpoints_->stale_binding = false;
    endpoints_->fail_subscriber_open = true;
    auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_FALSE(subscriber.ok());
    EXPECT_EQ(endpoints_->observed_subscribers, 1u);
    EXPECT_EQ(Usage(topic).subscribers, 0u);

    endpoints_->fail_subscriber_open = false;
    route_provider_->SetAvailable(false);
    auto refresh_failure = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_FALSE(refresh_failure.ok());
    EXPECT_EQ(refresh_failure.status().code(), StatusCode::kUnavailable);
    EXPECT_EQ(Usage(topic).subscribers, 0u);
}

TEST_F(BusTest, PublisherBindingFailureReleasesResourcesBeforeDeferredRollback) {
    const TopicId topic = CreateTopic();
    endpoints_->retain_last_endpoints = false;
    endpoints_->release_binding_after_open = true;
    endpoints_->stale_binding = true;
    fault_injector_->FailPublisherUnregisterAttempts(1);

    auto failed = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kInternal);
    EXPECT_TRUE(endpoints_->weak_last_publisher.expired());
    EXPECT_TRUE(endpoints_->weak_last_binding.expired());
    EXPECT_EQ(Usage(topic).publishers, 1u);

    auto retry_trigger = bus_->CreatePublisher(TopicId{}, TestSchema());
    ASSERT_FALSE(retry_trigger.ok());
    EXPECT_EQ(retry_trigger.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(Usage(topic).publishers, 0u);
}

TEST_F(BusTest, SubscriberRefreshFailureReleasesResourcesBeforeDeferredRollback) {
    const TopicId topic = CreateTopic();
    endpoints_->retain_last_endpoints = false;
    endpoints_->release_binding_after_open = true;
    route_provider_->SetAvailable(false);
    fault_injector_->FailSubscriberUnregisterAttempts(1);

    auto failed = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kInternal);
    EXPECT_TRUE(endpoints_->weak_last_subscriber.expired());
    EXPECT_TRUE(endpoints_->weak_last_binding.expired());
    EXPECT_EQ(Usage(topic).subscribers, 1u);

    auto retry_trigger = bus_->CreateSubscriber(TopicId{}, TestSchema());
    ASSERT_FALSE(retry_trigger.ok());
    EXPECT_EQ(retry_trigger.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(Usage(topic).subscribers, 0u);
}

TEST_F(BusTest, MoveOnlyEndpointsUnregisterAndOutliveBusAndProvider) {
    static_assert(!std::is_copy_constructible_v<BusPublisher>);
    static_assert(!std::is_copy_assignable_v<BusSubscriber>);

    const TopicId topic = CreateTopic();
    auto weak_binding = endpoints_->WeakBinding();
    {
        auto created_publisher = bus_->CreatePublisher(topic, TestSchema());
        ASSERT_TRUE(created_publisher.ok())
            << created_publisher.status().ToString();
        BusPublisher publisher = std::move(*created_publisher);

        auto created_subscriber = bus_->CreateSubscriber(topic, TestSchema());
        ASSERT_TRUE(created_subscriber.ok())
            << created_subscriber.status().ToString();
        BusSubscriber subscriber = std::move(*created_subscriber);

        EXPECT_EQ(Usage(topic).publishers, 1u);
        EXPECT_EQ(Usage(topic).subscribers, 1u);
        endpoints_->last_publisher.reset();
        endpoints_->last_subscriber.reset();
        endpoints_->DropBinding();
        bus_.reset();
        EXPECT_FALSE(weak_binding.expired());
        EXPECT_TRUE(publisher.active());
        EXPECT_TRUE(subscriber.active());

        const std::vector<std::byte> payload = {std::byte{1}};
        EXPECT_TRUE(publisher.Publish(payload).ok());
    }

    EXPECT_TRUE(weak_binding.expired());
    EXPECT_EQ(Usage(topic).publishers, 0u);
    EXPECT_EQ(Usage(topic).subscribers, 0u);
}

TEST_F(BusTest, ResolvesRouteThenPublishesLocallyAndDispatches) {
    const TopicId topic = CreateTopic();
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    const std::vector<std::byte> payload = {
        std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};
    auto result = publisher->Publish(payload, 7);
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    ASSERT_EQ(events_->size(), 2u);
    EXPECT_EQ((*events_)[0], "local-publish");
    EXPECT_EQ((*events_)[1], "dispatch");
    EXPECT_EQ(endpoints_->last_publisher->calls, 1u);
    EXPECT_EQ(dispatcher_->calls, 1u);
    EXPECT_EQ(dispatcher_->topic_id, topic);
    EXPECT_EQ(dispatcher_->priority, 7u);
    EXPECT_EQ(dispatcher_->payload, payload);
    EXPECT_EQ(result->publication.source.node_id, local_node_.value);
    EXPECT_EQ(result->publication.source.publisher_id,
              publisher->publisher_id().value);
    EXPECT_EQ(result->publication.sequence_num, 1u);
    EXPECT_TRUE(result->bridge_status.ok());
    ASSERT_NE(result->route, nullptr);
    ASSERT_EQ(result->route, dispatcher_->route);
    ASSERT_EQ(result->route->targets().size(), 1u);
    EXPECT_EQ(result->route->targets()[0].target_node, local_node_);
    EXPECT_TRUE(std::holds_alternative<transport::LocalTargetRoute>(
        result->route->targets()[0].transport));
}

TEST_F(BusTest, RouteFailureDoesNotPublishLocally) {
    const TopicId topic = CreateTopic();
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    route_provider_->SetAvailable(false);
    const std::vector<std::byte> payload = {std::byte{0x10}};
    auto result = publisher->Publish(payload);

    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kUnavailable);
    EXPECT_EQ(endpoints_->last_publisher->calls, 0u);
    EXPECT_EQ(dispatcher_->calls, 0u);
    EXPECT_TRUE(events_->empty());
}

TEST_F(BusTest, DispatchFailureAndExceptionReturnPublishedSequence) {
    const TopicId topic = CreateTopic();
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    const std::vector<std::byte> payload = {std::byte{0x22}};

    dispatcher_->dispatch_status =
        Status::Error(StatusCode::kUnavailable, "bridge unavailable");
    auto failed = publisher->Publish(payload);
    ASSERT_TRUE(failed.ok()) << failed.status().ToString();
    EXPECT_EQ(failed->publication.sequence_num, 1u);
    EXPECT_EQ(failed->bridge_status.code(), StatusCode::kUnavailable);
    ASSERT_NE(failed->route, nullptr);

    dispatcher_->throw_exception = true;
    auto threw = publisher->Publish(payload);
    ASSERT_TRUE(threw.ok()) << threw.status().ToString();
    EXPECT_EQ(threw->publication.sequence_num, 2u);
    EXPECT_EQ(threw->bridge_status.code(), StatusCode::kInternal);
    ASSERT_NE(threw->route, nullptr);
    EXPECT_EQ(endpoints_->last_publisher->calls, 2u);
    EXPECT_EQ(dispatcher_->calls, 2u);
}

TEST_F(BusTest, ExplicitCloseDisablesEndpointAndRetainsFailedRegistration) {
    const TopicId topic = CreateTopic();
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    std::weak_ptr<FakeLocalSubscriber> weak_subscriber =
        endpoints_->last_subscriber;
    endpoints_->last_subscriber.reset();
    EXPECT_TRUE(subscriber->Close().ok());
    EXPECT_FALSE(subscriber->active());
    EXPECT_TRUE(weak_subscriber.expired());
    EXPECT_EQ(Usage(topic).subscribers, 0u);
    EXPECT_TRUE(subscriber->Close().ok());

    std::weak_ptr<FakeLocalPublisher> weak_publisher =
        endpoints_->last_publisher;
    std::weak_ptr<const transport::LocalPublicationBinding> weak_binding =
        endpoints_->WeakBinding();
    endpoints_->last_publisher.reset();
    endpoints_->DropBinding();
    fault_injector_->FailPublisherUnregisterAttempts(1);

    const Status first_close = publisher->Close();
    EXPECT_EQ(first_close.code(), StatusCode::kUnavailable);
    EXPECT_FALSE(publisher->active());
    EXPECT_TRUE(weak_publisher.expired());
    EXPECT_FALSE(weak_binding.expired());
    EXPECT_EQ(Usage(topic).publishers, 1u);

    const std::vector<std::byte> payload = {std::byte{1}};
    auto after_close = publisher->Publish(payload);
    ASSERT_FALSE(after_close.ok());
    EXPECT_EQ(after_close.status().code(), StatusCode::kInvalidArgument);

    EXPECT_TRUE(publisher->Close().ok());
    EXPECT_EQ(Usage(topic).publishers, 0u);
    EXPECT_TRUE(weak_binding.expired());
    EXPECT_TRUE(publisher->Close().ok());
}

TEST_F(BusTest, DestructorRetriesTransientUnregisterFailures) {
    const TopicId topic = CreateTopic();
    {
        auto publisher = bus_->CreatePublisher(topic, TestSchema());
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
        BusPublisher moved = std::move(*publisher);
        EXPECT_FALSE(publisher->active());
        EXPECT_TRUE(moved.active());
        endpoints_->last_publisher.reset();
        fault_injector_->FailPublisherUnregisterAttempts(1);
    }
    EXPECT_EQ(Usage(topic).publishers, 0u);

    {
        auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
        ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
        BusSubscriber moved = std::move(*subscriber);
        EXPECT_FALSE(subscriber->active());
        EXPECT_TRUE(moved.active());
        endpoints_->last_subscriber.reset();
        fault_injector_->FailSubscriberUnregisterAttempts(1);
    }
    EXPECT_EQ(Usage(topic).subscribers, 0u);
}

TEST_F(BusTest, ProcessAnchorRetainsDebtAfterLastContextIsDestroyed) {
    const TopicId topic = CreateTopic();
    std::weak_ptr<FakeLocalPublisher> weak_publisher;
    std::weak_ptr<FakeLocalSubscriber> weak_subscriber;
    {
        auto publisher = bus_->CreatePublisher(topic, TestSchema());
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
        auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
        ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
        weak_publisher = endpoints_->last_publisher;
        weak_subscriber = endpoints_->last_subscriber;
        endpoints_->last_publisher.reset();
        endpoints_->last_subscriber.reset();
        fault_injector_->FailPublisherUnregisterAttempts(16);
        fault_injector_->FailSubscriberUnregisterAttempts(16);
        bus_.reset();
    }

    EXPECT_TRUE(weak_publisher.expired());
    EXPECT_TRUE(weak_subscriber.expired());
    EXPECT_EQ(Usage(topic).publishers, 1u);
    EXPECT_EQ(Usage(topic).subscribers, 1u);

    fault_injector_->FailPublisherUnregisterAttempts(0);
    fault_injector_->FailSubscriberUnregisterAttempts(0);
    auto replacement =
        Bus::Create(owner_, coordinator_, switcher_, ids_, endpoints_,
                    dispatcher_, fault_injector_);
    ASSERT_TRUE(replacement.ok()) << replacement.status().ToString();
    bus_ = std::move(*replacement);
    EXPECT_EQ(Usage(topic).publishers, 0u);
    EXPECT_EQ(Usage(topic).subscribers, 0u);
}

TEST_F(BusTest, SubscriberDelegatesTryPollToLocalSeam) {
    const TopicId topic = CreateTopic();
    auto subscriber = bus_->CreateSubscriber(topic, TestSchema());
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    const std::vector<std::byte> payload = {std::byte{4}, std::byte{5}};
    endpoints_->last_subscriber->messages.push_back(CanonicalMessage{
        .schema = TestSchema(),
        .publication = {
            .source = {.node_id = 2, .publisher_id = 3, .publisher_epoch = 4},
            .sequence_num = 9,
            .timestamp_ns = 10,
            .message_type = 11,
        },
        .priority = 6,
        .payload = payload,
    });

    auto message = subscriber->TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    EXPECT_EQ(message->payload, payload);
    EXPECT_EQ(message->publication.sequence_num, 9u);
    EXPECT_EQ(message->priority, 6u);
    EXPECT_EQ(endpoints_->last_subscriber->calls, 1u);

    auto empty = subscriber->TryPoll();
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
}

TEST_F(BusTest, RejectsOversizedCanonicalPayloadBeforeLocalPublish) {
    const TopicId topic = CreateTopic();
    auto publisher = bus_->CreatePublisher(topic, TestSchema());
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    std::vector<std::byte> oversized(kMaxBusCanonicalPayloadBytes + 1);
    auto result = publisher->Publish(oversized);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(endpoints_->last_publisher->calls, 0u);
    EXPECT_EQ(dispatcher_->calls, 0u);
}

}  // namespace
}  // namespace mino
