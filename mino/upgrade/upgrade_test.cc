// Copyright 2026 The Mino Authors

#include "mino/upgrade/manifest.h"
#include "mino/upgrade/orchestrator.h"
#include "mino/upgrade/routing_catalog.h"

#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mino::upgrade {
namespace {

schema::SchemaIdentity Schema(uint64_t id, uint32_t version) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(id);
    digest[1] = static_cast<std::byte>(version);
    return schema::SchemaIdentity(id, digest, version, 1);
}

registry::TopicAcl Acl() {
    return registry::TopicAcl{.entries = {{
                                  .node_id = NodeId{7},
                                  .security_domain_id = SecurityDomainId{11},
                                  .permissions = registry::kAllTopicPermissions,
                              }}};
}

TopicBinding Topic(uint32_t id, std::string name,
                   schema::SchemaIdentity schema) {
    return TopicBinding{
        .topic_id = TopicId{id},
        .name = std::move(name),
        .config_version = 5,
        .region_version = 3,
        .channel_version = 9,
        .acl_version = 2,
        .schema = schema,
        .acl = Acl(),
    };
}

UpgradePlan Plan() {
    return UpgradePlan{
        .operation_id = "upgrade-test-1",
        .commit_token = "0123456789abcdef0123456789abcdef",
        .source_region = {.name = "/mino-old",
                          .region_id = 10,
                          .uuid_lo = 101,
                          .uuid_hi = 102,
                          .layout_version = 5,
                          .security_domain = SecurityDomainId{11}},
        .target_region = {.name = "/mino-new",
                          .region_id = 20,
                          .uuid_lo = 201,
                          .uuid_hi = 202,
                          .layout_version = 6,
                          .security_domain = SecurityDomainId{11}},
        .topics = {{.source = Topic(1, "camera/image", Schema(31, 1)),
                    .target = Topic(2, "camera/image.next", Schema(32, 2))}},
        .required_shm_bytes = 4096,
        .required_publisher_slots = 1,
        .required_subscriber_slots = 2,
        .minimum_observation_samples = 3,
    };
}

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static uint64_t sequence = 0;
        path_ = std::filesystem::temp_directory_path() /
                ("mino-upgrade-test-" + std::to_string(::getpid()) + "-" +
                 std::to_string(++sequence));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    std::filesystem::path Manifest() const { return path_ / "upgrade.manifest"; }
    std::filesystem::path Catalog() const { return path_ / "region.routes"; }

private:
    std::filesystem::path path_;
};

class FakeControlPlane final : public UpgradeControlPlane {
public:
    Status Prepare(const UpgradePlan&) override {
        ++prepare_calls;
        return prepare_status;
    }
    Result<TargetReadinessProof> ObserveTarget(
        const UpgradePlan& plan) override {
        ++readiness_calls;
        if (!readiness_status.ok()) return readiness_status;
        TargetReadinessProof proof{
            .region = plan.target_region,
            .topics = {},
            .region_active = true,
            .processes_ready = true,
            .channels_ready = true,
            .routes_ready = true,
            .schema_bidirectionally_compatible = true,
            .acl_exactly_preserved = true,
            .capacity_admitted = true,
            .available_shm_bytes = plan.required_shm_bytes,
            .available_publisher_slots = plan.required_publisher_slots,
            .available_subscriber_slots = plan.required_subscriber_slots,
        };
        for (const TopicUpgrade& topic : plan.topics) {
            proof.topics.push_back(topic.target);
        }
        return proof;
    }
    Status BeginDrain(const UpgradePlan&) override {
        ++drain_calls;
        return drain_status;
    }
    Result<DrainProof> ObserveDrain(const UpgradePlan&) override {
        ++drain_observe_calls;
        return drain_proof;
    }
    Status Cutover(const UpgradePlan&) override {
        ++cutover_calls;
        return cutover_status;
    }
    Result<CutoverObservation> ObserveCutover(
        const UpgradePlan& plan) override {
        ++observe_calls;
        CutoverObservation value = observation;
        if (value.acknowledged_commit_token.empty()) {
            value.acknowledged_commit_token = plan.commit_token;
        }
        if (value.active_region.region_id == 0) value.active_region = plan.target_region;
        return value;
    }
    Status Commit(const UpgradePlan&) override {
        ++commit_calls;
        return commit_status;
    }
    Result<SafeRollbackProof> ObserveRollbackSafety(
        const UpgradePlan&) override {
        ++rollback_observe_calls;
        return rollback_proof;
    }
    Status Rollback(const UpgradePlan&, bool after_cutover) override {
        ++rollback_calls;
        rollback_was_after_cutover = after_cutover;
        return rollback_status;
    }

    Status prepare_status = Status::Ok();
    Status readiness_status = Status::Ok();
    Status drain_status = Status::Ok();
    Status cutover_status = Status::Ok();
    Status commit_status = Status::Ok();
    Status rollback_status = Status::Ok();
    DrainProof drain_proof{.old_publishers_fenced = true,
                           .last_published_sequence = 100,
                           .last_consumed_sequence = 100};
    CutoverObservation observation{.acknowledged_commit_token = {},
                                   .active_region = {},
                                   .old_publisher_count = 0,
                                   .new_publisher_count = 1,
                                   .observed_samples = 3};
    SafeRollbackProof rollback_proof{.target_publishers_fenced = true,
                                     .source_ready = true};
    int prepare_calls = 0;
    int readiness_calls = 0;
    int drain_calls = 0;
    int drain_observe_calls = 0;
    int cutover_calls = 0;
    int observe_calls = 0;
    int commit_calls = 0;
    int rollback_observe_calls = 0;
    int rollback_calls = 0;
    bool rollback_was_after_cutover = false;
};

TEST(UpgradeValidationTest, RejectsSecurityDomainOrAclWeakening) {
    UpgradePlan plan = Plan();
    plan.target_region.security_domain = SecurityDomainId{12};
    EXPECT_EQ(ValidateUpgradePlan(plan).code(), StatusCode::kPermissionDenied);

    plan = Plan();
    plan.topics[0].target.acl.entries[0].permissions =
        static_cast<uint32_t>(registry::TopicPermission::kSubscribe);
    EXPECT_EQ(ValidateUpgradePlan(plan).code(), StatusCode::kPermissionDenied);
}

TEST(UpgradeManifestTest, CodecRoundTripsAndDetectsCrcDamage) {
    UpgradeSnapshot snapshot{
        .generation = 1,
        .created_at_ns = 10,
        .updated_at_ns = 10,
        .phase = UpgradePhase::kPrepare,
        .plan = Plan(),
        .journal = {{.sequence = 1,
                     .phase = UpgradePhase::kPrepare,
                     .timestamp_ns = 10,
                     .detail = "created"}},
        .terminal_reason = {},
    };
    auto encoded = EncodeUpgradeSnapshot(snapshot);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    auto decoded = DecodeUpgradeSnapshot(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, snapshot);

    (*encoded)[20] ^= std::byte{1};
    EXPECT_EQ(DecodeUpgradeSnapshot(*encoded).status().code(),
              StatusCode::kCorruption);
}

TEST(RegionRoutingCatalogTest,
     DurableGenerationCasFencesSourceAndMakesCutoverTokenIdempotent) {
    TemporaryDirectory directory;
    UpgradePlan plan = Plan();
    auto catalog =
        RegionRoutingCatalog::Create(directory.Catalog(), plan.source_region);
    ASSERT_TRUE(catalog.ok()) << catalog.status().ToString();
    EXPECT_EQ((*catalog)->snapshot().generation, 1u);
    EXPECT_EQ((*catalog)->snapshot().active_region, plan.source_region);

    ASSERT_TRUE((*catalog)->FenceSource(plan).ok());
    EXPECT_EQ((*catalog)->snapshot().generation, 2u);
    EXPECT_TRUE((*catalog)->snapshot().source_fenced);
    EXPECT_EQ((*catalog)->snapshot().commit_token, plan.commit_token);
    ASSERT_TRUE((*catalog)->FenceSource(plan).ok());
    EXPECT_EQ((*catalog)->snapshot().generation, 2u);

    UpgradePlan other = plan;
    other.commit_token = "ffffffffffffffffffffffffffffffff";
    EXPECT_EQ((*catalog)->FenceSource(other).code(), StatusCode::kAlreadyExists);
    ASSERT_TRUE((*catalog)->Cutover(plan).ok());
    EXPECT_EQ((*catalog)->snapshot().generation, 3u);
    EXPECT_EQ((*catalog)->snapshot().active_region, plan.target_region);
    ASSERT_TRUE((*catalog)->Cutover(plan).ok());
    EXPECT_EQ((*catalog)->snapshot().generation, 3u);
    catalog->reset();

    auto recovered = RegionRoutingCatalog::Open(directory.Catalog());
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    EXPECT_EQ((*recovered)->snapshot().generation, 3u);
    EXPECT_EQ((*recovered)->snapshot().active_region, plan.target_region);
    RegionRoutingSnapshot stale = (*recovered)->snapshot();
    ++stale.generation;
    EXPECT_EQ((*recovered)
                  ->CompareExchange(1, plan.source_region, std::move(stale))
                  .status()
                  .code(),
              StatusCode::kAlreadyExists);
}

TEST(RegionRoutingCatalogTest, DetectsCrcDamage) {
    TemporaryDirectory directory;
    auto catalog = RegionRoutingCatalog::Create(directory.Catalog(),
                                                Plan().source_region);
    ASSERT_TRUE(catalog.ok());
    catalog->reset();
    std::fstream file(directory.Catalog(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.good());
    file.seekg(16);
    char value = 0;
    file.read(&value, 1);
    value ^= 1;
    file.seekp(16);
    file.write(&value, 1);
    file.close();
    EXPECT_EQ(RegionRoutingCatalog::Open(directory.Catalog()).status().code(),
              StatusCode::kCorruption);
}

struct FaultContext {
    UpgradePersistenceFaultPoint selected;
};

Status FailSelected(UpgradePersistenceFaultPoint point, void* context) noexcept {
    const auto* fault = static_cast<const FaultContext*>(context);
    return point == fault->selected
               ? Status::Error(StatusCode::kUnavailable, "injected crash cutpoint")
               : Status::Ok();
}

TEST(UpgradeManifestTest, EveryPersistenceCutpointRecoversOldOrNewGeneration) {
    constexpr std::array points = {
        UpgradePersistenceFaultPoint::kAfterTemporaryWrite,
        UpgradePersistenceFaultPoint::kAfterTemporaryDataSync,
        UpgradePersistenceFaultPoint::kAfterAtomicRename,
        UpgradePersistenceFaultPoint::kAfterParentDirectorySync,
    };
    for (const UpgradePersistenceFaultPoint point : points) {
        TemporaryDirectory directory;
        auto created = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        created->reset();

        FaultContext context{point};
        UpgradeManifestOptions options{.fault_hook = FailSelected,
                                       .fault_hook_context = &context};
        auto opened = UpgradeManifestStore::Open(directory.Manifest(), options);
        ASSERT_TRUE(opened.ok()) << opened.status().ToString();
        EXPECT_EQ((*opened)->Advance(UpgradePhase::kValidate, 20, "next").code(),
                  StatusCode::kUnavailable);
        opened->reset();

        auto recovered = UpgradeManifestStore::Open(directory.Manifest());
        ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
        const bool rename_happened =
            point == UpgradePersistenceFaultPoint::kAfterAtomicRename ||
            point == UpgradePersistenceFaultPoint::kAfterParentDirectorySync;
        EXPECT_EQ((*recovered)->snapshot().phase,
                  rename_happened ? UpgradePhase::kValidate
                                  : UpgradePhase::kPrepare);
        EXPECT_EQ((*recovered)->snapshot().generation, rename_happened ? 2u : 1u);
    }
}

TEST(UpgradeOrchestratorTest, VisitsEveryForwardStateAndCommits) {
    TemporaryDirectory directory;
    auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    FakeControlPlane control;
    UpgradeOrchestrator orchestrator(store->get(), &control);

    EXPECT_EQ(orchestrator.Preview().current, UpgradePhase::kPrepare);
    ASSERT_TRUE(orchestrator.Step(20).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kValidate);
    ASSERT_TRUE(orchestrator.Step(30).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kDrain);
    ASSERT_TRUE(orchestrator.Step(40).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kCutover);
    ASSERT_TRUE(orchestrator.Step(50).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kObserve);
    ASSERT_TRUE(orchestrator.Step(60).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kCommit);
    EXPECT_EQ((*store)->snapshot().journal.size(), 6u);
    EXPECT_EQ(control.prepare_calls, 1);
    EXPECT_EQ(control.drain_calls, 1);
    EXPECT_EQ(control.cutover_calls, 1);
    EXPECT_EQ(control.commit_calls, 1);
}

TEST(UpgradeOrchestratorTest, DrainCannotAdvanceOnAnyOutstandingClass) {
    const std::vector<DrainProof> incomplete = {
        {.publishers = 1},
        {.old_publishers_fenced = true, .subscribers = 1},
        {.old_publishers_fenced = true, .pins = 1},
        {.old_publishers_fenced = true, .outstanding_receipts = 1},
        {.old_publishers_fenced = true, .outstanding_borrows = 1},
        {.old_publishers_fenced = true, .queue_depth = 1},
        {.old_publishers_fenced = true,
         .last_published_sequence = 10,
         .last_consumed_sequence = 9},
    };
    for (const DrainProof& proof : incomplete) {
        TemporaryDirectory directory;
        auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
        ASSERT_TRUE(store.ok());
        FakeControlPlane control;
        UpgradeOrchestrator orchestrator(store->get(), &control);
        ASSERT_TRUE(orchestrator.Step(20).ok());
        ASSERT_TRUE(orchestrator.Step(30).ok());
        control.drain_proof = proof;
        EXPECT_EQ(orchestrator.Step(40).code(), StatusCode::kWouldBlock);
        EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kDrain);
    }
}

TEST(UpgradeOrchestratorTest, CutoverObservationFailsClosed) {
    TemporaryDirectory directory;
    auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
    ASSERT_TRUE(store.ok());
    FakeControlPlane control;
    UpgradeOrchestrator orchestrator(store->get(), &control);
    ASSERT_TRUE(orchestrator.Step(20).ok());
    ASSERT_TRUE(orchestrator.Step(30).ok());
    ASSERT_TRUE(orchestrator.Step(40).ok());
    ASSERT_TRUE(orchestrator.Step(50).ok());

    control.observation.duplicate_count = 1;
    EXPECT_EQ(orchestrator.Step(60).code(), StatusCode::kDegraded);
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kObserve);
    EXPECT_EQ(control.commit_calls, 0);
}

TEST(UpgradeOrchestratorTest, PreCutoverRollbackNeedsNoPostCutoverProof) {
    TemporaryDirectory directory;
    auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
    ASSERT_TRUE(store.ok());
    FakeControlPlane control;
    UpgradeOrchestrator orchestrator(store->get(), &control);
    ASSERT_TRUE(orchestrator.Step(20).ok());
    ASSERT_TRUE(orchestrator.Rollback(30).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kRollback);
    EXPECT_EQ(control.rollback_observe_calls, 0);
    EXPECT_FALSE(control.rollback_was_after_cutover);
}

TEST(UpgradeOrchestratorTest, PostCutoverRollbackRequiresExplicitSafetyProof) {
    TemporaryDirectory directory;
    auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
    ASSERT_TRUE(store.ok());
    FakeControlPlane control;
    UpgradeOrchestrator orchestrator(store->get(), &control);
    ASSERT_TRUE(orchestrator.Step(20).ok());
    ASSERT_TRUE(orchestrator.Step(30).ok());
    ASSERT_TRUE(orchestrator.Step(40).ok());
    ASSERT_TRUE(orchestrator.Step(50).ok());
    ASSERT_EQ((*store)->snapshot().phase, UpgradePhase::kObserve);

    control.rollback_proof.target_publications_after_cutover = 1;
    EXPECT_EQ(orchestrator.Rollback(60).code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(control.rollback_calls, 0);

    control.rollback_proof.sequence_receipt_reconciliation_complete = true;
    ASSERT_TRUE(orchestrator.Rollback(70).ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kRollback);
    EXPECT_TRUE(control.rollback_was_after_cutover);
}

TEST(UpgradeOrchestratorTest, ExplicitFailIsDurableTerminalState) {
    TemporaryDirectory directory;
    auto store = UpgradeManifestStore::Create(directory.Manifest(), Plan(), 10);
    ASSERT_TRUE(store.ok());
    FakeControlPlane control;
    UpgradeOrchestrator orchestrator(store->get(), &control);
    ASSERT_TRUE(orchestrator.Fail(20, "operator quarantined target").ok());
    EXPECT_EQ((*store)->snapshot().phase, UpgradePhase::kFail);
    EXPECT_EQ((*store)->snapshot().terminal_reason,
              "operator quarantined target");
}

}  // namespace
}  // namespace mino::upgrade
