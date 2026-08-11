// Copyright 2026 The Mino Authors

#include "mino/upgrade/manifest.h"
#include "mino/upgrade/orchestrator.h"
#include "mino/upgrade/production_control_plane.h"
#include "mino/upgrade/routing_catalog.h"

#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "mino/platform/shared_memory.h"
#include "mino/runtime/deployment/local_bus.h"
#include "mino/runtime/deployment/upgrade_adapter.h"

namespace mino::upgrade {
namespace {

schema::SchemaIdentity IntegrationSchema() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{0x42};
    return schema::SchemaIdentity(0x42, digest, 1, 1);
}

TopicBinding Binding(const registry::TopicMetadata& metadata) {
    return TopicBinding{
        .topic_id = metadata.topic_id,
        .name = metadata.name,
        .config_version = metadata.config_version,
        .region_version = metadata.region_version,
        .channel_version = metadata.channel_version,
        .acl_version = metadata.acl_version,
        .schema = metadata.schema,
        .acl = metadata.acl,
    };
}

std::array<std::byte, sizeof(uint64_t)> Payload(uint64_t value) {
    std::array<std::byte, sizeof(uint64_t)> payload{};
    std::memcpy(payload.data(), &value, sizeof(value));
    return payload;
}

uint64_t PayloadValue(const CanonicalMessage& message) {
    uint64_t value = 0;
    EXPECT_EQ(message.payload.size(), sizeof(value));
    if (message.payload.size() == sizeof(value)) {
        std::memcpy(&value, message.payload.data(), sizeof(value));
    }
    return value;
}

struct OneShotFault {
    bool fired = false;
};

Status FailAfterCutoverSideEffect(UpgradePersistenceFaultPoint point,
                                  void* context) noexcept {
    auto* fault = static_cast<OneShotFault*>(context);
    if (point == UpgradePersistenceFaultPoint::kAfterTemporaryWrite &&
        !fault->fired) {
        fault->fired = true;
        return Status::Error(StatusCode::kUnavailable,
                             "simulated crash after durable catalog cutover");
    }
    return Status::Ok();
}

TEST(RollingUpgradeIntegrationTest,
     PublisherDrainCutoverCrashResumeHasNoDuplicateOrUnexplainedLoss) {
    const std::filesystem::path root =
        std::filesystem::temp_directory_path() /
        ("mino-production-upgrade-" + std::to_string(::getpid()));
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    ASSERT_TRUE(std::filesystem::create_directories(root));

    const std::string source_name =
        "/mino_upgrade_source_" + std::to_string(::getpid());
    const std::string target_name =
        "/mino_upgrade_target_" + std::to_string(::getpid());
    static_cast<void>(SharedMemorySegment::Unlink(source_name));
    static_cast<void>(SharedMemorySegment::Unlink(target_name));

    RegionCreateOptions source_options;
    source_options.name = source_name;
    source_options.size_bytes = 2u * 1024u * 1024u;
    source_options.security_domain = SecurityDomainId{77};
    auto source_created = SharedMemoryRegion::Create(source_options);
    ASSERT_TRUE(source_created.ok()) << source_created.status().ToString();
    SharedMemoryRegion source_region = std::move(*source_created);

    RegionCreateOptions target_options = source_options;
    target_options.name = target_name;
    auto target_created =
        ProductionUpgradeControlPlane::ProvisionTarget(target_options);
    ASSERT_TRUE(target_created.ok()) << target_created.status().ToString();
    SharedMemoryRegion target_region = std::move(target_created->region);

    const RegionIdentity source_identity =
        RegionIdentityFrom(source_name, source_region);
    const RegionIdentity target_identity = target_created->identity;
    const schema::SchemaIdentity schema = IntegrationSchema();

    deployment::LocalBusConfig bus_config{
        .node_id = NodeId{91},
        .security_domain_id = SecurityDomainId{77},
        .lease_epoch = 1,
        .lease_duration_ns = 60ull * 1'000'000'000ull,
        .region_id = source_identity.region_id,
        .region_bytes = 4u * 1024u * 1024u,
        .topic_id_state_path = root / "topic_ids",
        .topics = {
            deployment::LocalTopicConfig{
                .name = "upgrade/source",
                .schema = schema,
                .channel_capacity = 64,
                .max_subscribers = 4,
                .max_payload_bytes = 64,
                .region_id = source_identity.region_id,
                .activate = true,
            },
            deployment::LocalTopicConfig{
                .name = "upgrade/target",
                .schema = schema,
                .channel_capacity = 64,
                .max_subscribers = 4,
                .max_payload_bytes = 64,
                .region_id = target_identity.region_id,
                .activate = false,
            },
        },
    };
    auto deployment_created =
        deployment::LocalBusDeployment::Create(std::move(bus_config));
    ASSERT_TRUE(deployment_created.ok())
        << deployment_created.status().ToString();
    std::unique_ptr<deployment::LocalBusDeployment> local =
        std::move(*deployment_created);

    auto source_topic = local->coordinator().FindTopic("upgrade/source");
    auto target_topic = local->coordinator().FindTopic("upgrade/target");
    ASSERT_TRUE(source_topic.ok()) << source_topic.status().ToString();
    ASSERT_TRUE(target_topic.ok()) << target_topic.status().ToString();

    UpgradePlan plan{
        .operation_id = "production-integration-upgrade",
        .commit_token = "feedfacefeedfacefeedfacefeedface",
        .source_region = source_identity,
        .target_region = target_identity,
        .topics = {{.source = Binding((*source_topic)->metadata),
                    .target = Binding((*target_topic)->metadata)}},
        .required_shm_bytes = 1,
        .required_publisher_slots = 1,
        .required_subscriber_slots = 1,
        .minimum_observation_samples = 32,
    };
    ASSERT_TRUE(ValidateUpgradePlan(plan).ok());

    const std::filesystem::path catalog_path = root / "region.routes";
    auto catalog = RegionRoutingCatalog::Create(catalog_path, source_identity);
    ASSERT_TRUE(catalog.ok()) << catalog.status().ToString();
    auto adapter = deployment::LocalBusProductionUpgradeAdapter::Create(
        local.get(), catalog_path, &source_region, &target_region);
    ASSERT_TRUE(adapter.ok()) << adapter.status().ToString();
    auto control = ProductionUpgradeControlPlane::Create(
        &local->coordinator(), catalog->get(), adapter->get());
    ASSERT_TRUE(control.ok()) << control.status().ToString();

    std::vector<uint64_t> delivered;
    auto source_subscriber =
        local->bus().CreateSubscriber(plan.topics[0].source.topic_id, schema);
    auto source_publisher =
        local->bus().CreatePublisher(plan.topics[0].source.topic_id, schema);
    ASSERT_TRUE(source_subscriber.ok())
        << source_subscriber.status().ToString();
    ASSERT_TRUE(source_publisher.ok()) << source_publisher.status().ToString();
    for (uint64_t sequence = 1; sequence <= 32; ++sequence) {
        const auto payload = Payload(sequence);
        ASSERT_TRUE(source_publisher->Publish(payload).ok());
        auto message = source_subscriber->TryPoll();
        ASSERT_TRUE(message.ok()) << message.status().ToString();
        delivered.push_back(PayloadValue(*message));
    }

    const std::filesystem::path manifest = root / "upgrade.manifest";
    auto store = UpgradeManifestStore::Create(manifest, plan, 10);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    UpgradeOrchestrator orchestrator(store->get(), control->get());
    ASSERT_TRUE(orchestrator.Step(20).ok());  // real Region attach + prepare
    ASSERT_TRUE(orchestrator.Step(30).ok());  // readiness + Coordinator drain
    EXPECT_EQ(local->bus()
                  .CreatePublisher(plan.topics[0].source.topic_id, schema)
                  .status()
                  .code(),
              StatusCode::kUnavailable);
    ASSERT_TRUE(source_publisher->Close().ok());
    ASSERT_TRUE(source_subscriber->Close().ok());
    ASSERT_TRUE(orchestrator.Step(40).ok());  // real channel conservation
    ASSERT_EQ((*store)->snapshot().phase, UpgradePhase::kCutover);
    store->reset();

    OneShotFault fault;
    UpgradeManifestOptions fault_options{
        .fault_hook = FailAfterCutoverSideEffect,
        .fault_hook_context = &fault,
    };
    auto crash_store = UpgradeManifestStore::Open(manifest, fault_options);
    ASSERT_TRUE(crash_store.ok()) << crash_store.status().ToString();
    UpgradeOrchestrator crashing(crash_store->get(), control->get());
    EXPECT_EQ(crashing.Step(50).code(), StatusCode::kUnavailable);
    EXPECT_EQ((*catalog)->snapshot().active_region, target_identity);
    EXPECT_EQ((*catalog)->snapshot().commit_token, plan.commit_token);
    crash_store->reset();

    auto target_subscriber =
        local->bus().CreateSubscriber(plan.topics[0].target.topic_id, schema);
    auto target_publisher =
        local->bus().CreatePublisher(plan.topics[0].target.topic_id, schema);
    ASSERT_TRUE(target_subscriber.ok())
        << target_subscriber.status().ToString();
    ASSERT_TRUE(target_publisher.ok()) << target_publisher.status().ToString();
    for (uint64_t sequence = 33; sequence <= 64; ++sequence) {
        const auto payload = Payload(sequence);
        ASSERT_TRUE(target_publisher->Publish(payload).ok());
        auto message = target_subscriber->TryPoll();
        ASSERT_TRUE(message.ok()) << message.status().ToString();
        delivered.push_back(PayloadValue(*message));
    }

    auto resumed_store = UpgradeManifestStore::Open(manifest);
    ASSERT_TRUE(resumed_store.ok()) << resumed_store.status().ToString();
    ASSERT_EQ((*resumed_store)->snapshot().phase, UpgradePhase::kCutover);
    UpgradeOrchestrator resumed(resumed_store->get(), control->get());
    ASSERT_TRUE(resumed.Step(60).ok());  // catalog says side effect happened
    ASSERT_TRUE(resumed.Step(70).ok());  // real observe + retire/delete + close
    EXPECT_EQ((*resumed_store)->snapshot().phase, UpgradePhase::kCommit);

    ASSERT_EQ(delivered.size(), 64u);
    for (uint64_t sequence = 1; sequence <= delivered.size(); ++sequence) {
        EXPECT_EQ(delivered[sequence - 1], sequence);
    }
    auto deleted_source =
        local->coordinator().GetTopic(plan.topics[0].source.topic_id);
    ASSERT_TRUE(deleted_source.ok());
    EXPECT_EQ((*deleted_source)->metadata.state, registry::TopicState::kDeleted);
    RegionAttachOptions closed_source_options;
    closed_source_options.name = source_name;
    closed_source_options.region_id = source_identity.region_id;
    closed_source_options.read_only = true;
    closed_source_options.security_domain = source_identity.security_domain;
    auto closed_source = SharedMemoryRegion::Attach(closed_source_options);
    ASSERT_TRUE(closed_source.ok()) << closed_source.status().ToString();
    EXPECT_TRUE(LoadCleanShutdown(*closed_source->superblock()));
    EXPECT_EQ(LoadRegionState(*closed_source->superblock()), RegionState::kClosed);
    ASSERT_TRUE(closed_source->Detach().ok());

    ASSERT_TRUE(target_publisher->Close().ok());
    ASSERT_TRUE(target_subscriber->Close().ok());
    ASSERT_TRUE(target_region.Detach().ok());
    resumed_store->reset();
    control->reset();
    adapter->reset();
    catalog->reset();
    local.reset();
    static_cast<void>(SharedMemorySegment::Unlink(source_name));
    static_cast<void>(SharedMemorySegment::Unlink(target_name));
    std::filesystem::remove_all(root, ignored);
}

}  // namespace
}  // namespace mino::upgrade
