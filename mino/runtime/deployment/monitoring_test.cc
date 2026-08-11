// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/monitoring.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/security/test_tls_credentials.h"

namespace mino::deployment {
namespace {

class TestSink final : public observability::OtlpJsonSink {
public:
    explicit TestSink(bool accept) noexcept : accept_(accept) {}

    bool TryBegin() noexcept override {
        size_ = 0;
        return accept_;
    }
    bool TryAppend(std::span<const char> fragment) noexcept override {
        if (fragment.size() > bytes_.size() - size_) return false;
        std::copy(fragment.begin(), fragment.end(), bytes_.begin() + size_);
        size_ += fragment.size();
        return true;
    }
    void Commit() noexcept override {
        commits.fetch_add(1, std::memory_order_release);
    }
    void Abort() noexcept override { aborts.fetch_add(1); }

    std::atomic<uint64_t> commits{0};
    std::atomic<uint64_t> aborts{0};

private:
    bool accept_;
    std::array<char, 64u * 1024u> bytes_{};
    size_t size_ = 0;
};

std::string Scrape(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    EXPECT_GE(fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address),
                        sizeof(address)),
              0);
    constexpr char kRequest[] = "GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n";
    EXPECT_EQ(::send(fd, kRequest, sizeof(kRequest) - 1, 0),
              static_cast<ssize_t>(sizeof(kRequest) - 1));
    std::string response;
    char buffer[4096];
    for (;;) {
        const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
        if (received <= 0) break;
        response.append(buffer, static_cast<size_t>(received));
    }
    (void)::close(fd);
    return response;
}

uint64_t CounterValue(const observability::TelemetrySnapshot& snapshot,
                      std::string_view name) {
    for (size_t i = 0; i < snapshot.counter_count; ++i) {
        if (snapshot.counters[i].name.view() == name) {
            return snapshot.counters[i].value;
        }
    }
    return 0;
}

bool WaitForMetrics(uint16_t port,
                    std::initializer_list<std::string_view> expected,
                    std::string* response) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(4);
    do {
        *response = Scrape(port);
        bool complete = true;
        for (std::string_view text : expected) {
            complete = complete && response->find(text) != std::string::npos;
        }
        if (complete) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

struct TestArtifact {
    schema::SchemaIdentity identity;
    std::vector<std::byte> bytes;
};

Result<TestArtifact> CompileArtifact() {
    MINO_ASSIGN_OR_RETURN(
        auto compiled,
        schema::SchemaCompiler::Compile(
            "option schema_version = \"1.0\"; package monitoring; "
            "message StorageFault { uint64 value = 1; }"));
    MINO_ASSIGN_OR_RETURN(
        auto layout,
        schema::LayoutPlanner::Plan(*compiled.types().front()));
    const std::array<schema::LayoutPlan, 1> layouts = {std::move(layout)};
    MINO_ASSIGN_OR_RETURN(
        auto encoded,
        schema::codegen::EncodeDescriptorArtifact(compiled, layouts));
    const auto bytes = std::as_bytes(
        std::span<const char>(encoded.data(), encoded.size()));
    return TestArtifact{
        .identity = compiled.types().front()->identity(),
        .bytes = std::vector<std::byte>(bytes.begin(), bytes.end()),
    };
}

uint32_t Crc32c(std::span<const std::byte> bytes) {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

std::ptrdiff_t FailStorageWrite(int, const std::byte*, size_t,
                                void*) noexcept {
    errno = EIO;
    return -1;
}

schema::SchemaIdentity TestSchema() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{0x5a};
    return schema::SchemaIdentity(0x5a, digest, 1, 1);
}

std::filesystem::path TestPath(std::string_view suffix) {
    static std::atomic<uint64_t> sequence{0};
    const char* test_tmpdir = std::getenv("TEST_TMPDIR");
    const std::filesystem::path root =
        test_tmpdir == nullptr ? std::filesystem::temp_directory_path()
                               : std::filesystem::path(test_tmpdir);
    return root / ("mino_monitoring_" + std::string(suffix) + "_" +
                   std::to_string(::getpid()) + "_" +
                   std::to_string(sequence.fetch_add(1)));
}

struct AlignedDelete {
    void operator()(std::byte* value) const noexcept {
        ::operator delete[](value, std::align_val_t(64));
    }
};

MonitoringConfig TestConfig() {
    MonitoringConfig config;
    config.prometheus.port = 0;
    config.prometheus.worker_threads = 1;
    config.prometheus.connection_limit = 4;
    config.prometheus.read_timeout_ms = 200;
    config.prometheus.write_timeout_ms = 500;
    config.prometheus.accept_poll_ms = 20;
    config.aggregate_interval_ms = 100;
    return config;
}

TEST(MonitoringDeploymentTest, AssemblesPrometheusAndBoundedOtlpWorkers) {
    TestSink sink(/*accept=*/true);
    MonitoringConfig config = TestConfig();
    config.otlp_enabled = true;
    auto created = MonitoringDeployment::Create(config, &sink);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto monitoring = std::move(*created);
    ASSERT_TRUE(monitoring->Start().ok());
    ASSERT_NE(monitoring->prometheus_port(), 0);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (sink.commits.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(sink.commits.load(std::memory_order_acquire), 0u);

    const std::string response = Scrape(monitoring->prometheus_port());
    EXPECT_NE(response.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(response.find("mino_otlp_export_queue_capacity 16\n"),
              std::string::npos);
    EXPECT_NE(response.find("mino_monitoring_up 1\n"), std::string::npos);
    monitoring->Stop();
    EXPECT_FALSE(monitoring->running());
}

TEST(MonitoringDeploymentTest, OtlpSinkFailureIsCountedAndFailSafe) {
    TestSink sink(/*accept=*/false);
    MonitoringConfig config = TestConfig();
    config.prometheus_enabled = false;
    config.otlp_enabled = true;
    auto created = MonitoringDeployment::Create(config, &sink);
    ASSERT_TRUE(created.ok());
    auto monitoring = std::move(*created);
    ASSERT_TRUE(monitoring->Start().ok());

    uint64_t failures = 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (failures == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        observability::TelemetrySnapshot snapshot;
        monitoring->registry().TakeSnapshot(1, &snapshot);
        failures = CounterValue(snapshot, "mino_otlp_export_failures_total");
    }
    EXPECT_GT(failures, 0u);
    monitoring->metrics().queue_dropped_total->counter().Increment(0);
    monitoring->Stop();
}

TEST(MonitoringDeploymentTest,
     RealModuleFailuresChangeLabelFreePrometheusMetrics) {
    const schema::SchemaIdentity schema = TestSchema();
    const std::filesystem::path state_path = TestPath("topic_ids");
    LocalBusConfig bus_config;
    bus_config.node_id = NodeId{701};
    bus_config.security_domain_id = SecurityDomainId{17};
    bus_config.lease_duration_ns = 60ull * 1'000'000'000ull;
    bus_config.region_id = 9;
    bus_config.topic_id_state_path = state_path;
    bus_config.topics = {LocalTopicConfig{
        .name = "monitoring/faults",
        .schema = schema,
        .channel_capacity = 2,
        .max_subscribers = 1,
        .max_payload_bytes = 64,
    }};
    auto bus_created = LocalBusDeployment::Create(std::move(bus_config));
    ASSERT_TRUE(bus_created.ok()) << bus_created.status().ToString();
    auto local_bus = std::move(*bus_created);
    auto subscriber =
        local_bus->bus().CreateSubscriber("monitoring/faults", schema);
    auto publisher = local_bus->bus().CreatePublisher("monitoring/faults", schema);
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    constexpr size_t kSlabBytes = 1u << 20;
    std::unique_ptr<std::byte[], AlignedDelete> slab_memory(
        new (std::align_val_t(64)) std::byte[kSlabBytes]);
    std::memset(slab_memory.get(), 0, kSlabBytes);
    ClassTableConfig slab_config;
    slab_config.classes = {{.slot_size = 64, .slot_count = 2}};
    auto slab_created = CentralSlabAllocator::Create(
        slab_memory.get(), kSlabBytes, slab_config);
    ASSERT_TRUE(slab_created.ok()) << slab_created.status().ToString();
    CentralSlabAllocator slab = *slab_created;

    capacity::NodeBudget budget;
    budget.limit.threads = 1;
    auto capacity_created = capacity::CapacityController::Create(budget);
    ASSERT_TRUE(capacity_created.ok());
    auto capacity = *capacity_created;

    const std::array principals = {
        security::testing::TestPrincipal{NodeId{801}, SecurityDomainId{17}},
    };
    auto credentials =
        security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(credentials.ok());
    auto provider = security::StaticTlsCredentialProvider::Create(
        std::move((*credentials)[0]));
    ASSERT_TRUE(provider.ok());
    auto tls_factory = security::CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_TRUE(tls_factory.ok()) << tls_factory.status().ToString();

    MonitoringSources sources;
    sources.local_bus = local_bus.get();
    sources.capacity = capacity.get();
    sources.tls = tls_factory->get();
    sources.slab_allocators = {&slab};
    auto monitoring_created =
        MonitoringDeployment::Create(TestConfig(), std::move(sources));
    ASSERT_TRUE(monitoring_created.ok())
        << monitoring_created.status().ToString();
    auto monitoring = std::move(*monitoring_created);
    ASSERT_TRUE(monitoring->Start().ok());

    std::string response;
    ASSERT_TRUE(WaitForMetrics(
        monitoring->prometheus_port(),
        {"mino_queue_full_total 0\n",
         "mino_slab_allocation_failures_total 0\n",
         "mino_capacity_rejections_total 0\n",
         "mino_tls_handshake_failures_total 0\n"},
        &response));

    const std::array<std::byte, 1> payload = {std::byte{0x1}};
    ASSERT_TRUE(publisher->Publish(payload).ok());
    ASSERT_TRUE(publisher->Publish(payload).ok());
    auto queue_full = publisher->Publish(payload);
    ASSERT_FALSE(queue_full.ok());
    EXPECT_EQ(queue_full.status().code(), StatusCode::kWouldBlock);

    AllocationRequest request;
    request.object_size = 32;
    request.type_id = TypeId{1};
    request.schema = SchemaIdentity{.short_id = 1, .layout_version = 1};
    ASSERT_TRUE(slab.Allocate(request).ok());
    ASSERT_TRUE(slab.Allocate(request).ok());
    EXPECT_EQ(slab.Allocate(request).status().code(),
              StatusCode::kResourceExhausted);

    capacity::ResourceVector excessive;
    excessive.threads = 2;
    EXPECT_EQ(capacity
                  ->Reserve(capacity::ResourceRequest{
                      .resources = excessive,
                      .scope = capacity::ResourceScope::kOther,
                      .admission_class = capacity::AdmissionClass::kDataPlane,
                      .name = "monitoring drill",
                  })
                  .status()
                  .code(),
              StatusCode::kResourceExhausted);

    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    const int flags = ::fcntl(sockets[0], F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(sockets[0], F_SETFL, flags | O_NONBLOCK), 0);
    auto tls = (*tls_factory)->Create(sockets[0], security::TlsRole::kClient);
    ASSERT_TRUE(tls.ok());
    ASSERT_EQ(::close(sockets[1]), 0);
    sockets[1] = -1;
    struct sigaction ignored {};
    struct sigaction previous {};
    ignored.sa_handler = SIG_IGN;
    ASSERT_EQ(::sigemptyset(&ignored.sa_mask), 0);
    ASSERT_EQ(::sigaction(SIGPIPE, &ignored, &previous), 0);
    EXPECT_FALSE((*tls)->Handshake().ok());
    ASSERT_EQ(::sigaction(SIGPIPE, &previous, nullptr), 0);
    ASSERT_EQ(::close(sockets[0]), 0);

    ASSERT_TRUE(WaitForMetrics(
        monitoring->prometheus_port(),
        {"mino_queue_depth 2\n", "mino_queue_capacity 2\n",
         "mino_queue_near_full_total 1\n", "mino_queue_full_total 1\n",
         "mino_queue_dropped_total 1\n", "mino_slab_allocations_total 2\n",
         "mino_slab_allocation_failures_total 1\n",
         "mino_capacity_rejections_total 1\n",
         "mino_tls_handshake_failures_total 1\n"},
        &response))
        << response;
    EXPECT_EQ(response.find("node="), std::string::npos);
    EXPECT_EQ(response.find("topic="), std::string::npos);
    EXPECT_EQ(response.find("cert="), std::string::npos);

    EXPECT_EQ(local_bus->SweepExpiredSubscribers(
                  std::numeric_limits<uint64_t>::max()),
              1u);
    ASSERT_TRUE(WaitForMetrics(monitoring->prometheus_port(),
                               {"mino_lease_expirations_total 1\n",
                                "mino_queue_depth 0\n"},
                               &response));
    monitoring->Stop();
    std::error_code ignored_error;
    std::filesystem::remove(state_path, ignored_error);
}

TEST(MonitoringDeploymentTest,
     RecorderBacklogAndRealWriteFailureReachPrometheus) {
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestPath("recorder");
    auto recorder_created = storage::Recorder::Create(
        root,
        storage::RecordingSessionMetadata{
            .recording_id = 1,
            .created_at_ns = 10,
            .owner_id = 20,
            .owner_epoch = 30,
            .config_version = 1,
        });
    ASSERT_TRUE(recorder_created.ok())
        << recorder_created.status().ToString();
    auto recorder = std::move(*recorder_created);
    storage::RecorderTopicConfig topic;
    topic.topic_id = TopicId{1};
    topic.topic_name = "monitoring-storage-fault";
    topic.config_version = 1;
    topic.segment_options.write_hook = FailStorageWrite;
    topic.schemas.push_back(storage::RecorderTopicSchema{
        .identity = artifact->identity,
        .descriptor_artifact = artifact->bytes,
    });
    ASSERT_TRUE(recorder->AddTopic(topic).ok());
    ASSERT_TRUE(recorder->Start(1000).ok());

    MonitoringSources sources;
    sources.recorder = recorder.get();
    auto monitoring_created =
        MonitoringDeployment::Create(TestConfig(), std::move(sources));
    ASSERT_TRUE(monitoring_created.ok());
    auto monitoring = std::move(*monitoring_created);
    ASSERT_TRUE(monitoring->Start().ok());

    const std::array<std::byte, 2> payload = {std::byte{1}, std::byte{2}};
    const storage::RecorderRecordMetadata metadata{
        .schema = storage::RecorderSchemaMetadata{
            .short_id = artifact->identity.short_id(),
            .canonical_digest = artifact->identity.canonical_digest(),
            .schema_version = artifact->identity.schema_version(),
            .layout_version = artifact->identity.layout_version(),
        },
        .topic_id = TopicId{1},
        .source = storage::MessageSource{
            .node_id = 1,
            .publisher_id = 2,
            .publisher_epoch = 3,
            .source_sequence = 1,
            .observed_timestamp_ns = 100,
        },
        .ingestion_timestamp_ns = 1001,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = Crc32c(payload),
    };
    auto enqueued = recorder->Enqueue(0, metadata, payload);
    ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
    ASSERT_EQ(enqueued->disposition,
              storage::RecorderEnqueueDisposition::kBuffered);

    std::string response;
    ASSERT_TRUE(WaitForMetrics(monitoring->prometheus_port(),
                               {"mino_storage_pending_bytes 4096\n"},
                               &response))
        << response;
    auto pumped = recorder->Pump(1002);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    ASSERT_EQ(pumped->failures.size(), 1u);
    ASSERT_TRUE(WaitForMetrics(
        monitoring->prometheus_port(),
        {"mino_storage_pending_bytes 0\n",
         "mino_storage_write_failures_total 1\n",
         "mino_storage_sync_failures_total 0\n",
         "mino_storage_errors_total 1\n"},
        &response))
        << response;
    monitoring->Stop();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
}

TEST(MonitoringDeploymentTest, RequiresSinkWhenOtlpIsEnabled) {
    MonitoringConfig config = TestConfig();
    config.otlp_enabled = true;
    auto created = MonitoringDeployment::Create(config, nullptr);
    EXPECT_FALSE(created.ok());
    EXPECT_EQ(created.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::deployment
