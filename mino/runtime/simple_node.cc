// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <atomic>
#include <new>
#include <cstddef>
#include <span>
#include <string>
#include <type_traits>

#include <chrono>
#include <cstring>
#include <limits>
#include <optional>

#include <thread>
#include <utility>

#include <sys/resource.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include "mino/common/checked_arithmetic.h"
#include "mino/common/ids.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/bitmap.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/class_table.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {
namespace {

constexpr uint64_t kCacheLine = 64;
constexpr uint64_t kMagic = 0x4D494E4F534D5031ull;  // "MINOSMP1"
constexpr uint32_t kVersion = 1;
constexpr uint32_t kMaxTopicSlots = 16;
constexpr uint32_t kMaxTopicName = 63;
constexpr uint32_t kMinQueueDepth = 2;
constexpr uint32_t kMaxQueueDepth = 1024;
constexpr uint32_t kMaxPayloadBytes = 1024u * 1024u;
constexpr uint32_t kSimpleMsgType = 0x53424C42u;  // "SBLB"
constexpr uint32_t kSimpleSchemaVersion = (1u << 16);
constexpr uint64_t kSimpleSchemaShortId = 0x4D494E4F42594453ull;
constexpr uint32_t kSimpleLayoutVersion = 1;
constexpr uint32_t kTopicEmpty = 0;
constexpr uint32_t kTopicClaiming = 1;
constexpr uint32_t kTopicReady = 2;
constexpr uint64_t kMarkerSlackBytes = 8192;

struct alignas(kCacheLine) TopicSlot {
    std::atomic<uint32_t> state{0};
    std::atomic<uint32_t> subscriber_claimed{0};
    char name[64]{};
    uint64_t channel_offset = 0;
    uint64_t channel_extent = 0;
    uint64_t capacity = 0;
    uint32_t reserved[2]{};
    unsigned char pad[24]{};
};

static_assert(sizeof(TopicSlot) == 128);
static_assert(alignof(TopicSlot) == kCacheLine);
static_assert(std::is_standard_layout_v<TopicSlot>);

struct alignas(kCacheLine) ManifestHeader {
    std::atomic<uint64_t> magic{0};
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t total_size = 0;
    uint32_t topic_slots = 0;
    uint32_t queue_depth = 0;
    uint32_t max_payload_bytes = 0;
    uint32_t reserved0 = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    unsigned char pad[56]{};
    TopicSlot topics[kMaxTopicSlots];
};

static_assert(sizeof(ManifestHeader) == 128 + kMaxTopicSlots * 128);
static_assert(alignof(ManifestHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<ManifestHeader>);
static_assert(offsetof(ManifestHeader, magic) == 0);
static_assert(offsetof(ManifestHeader, topics) == 128);

struct SegmentLayout {
    uint64_t total_size = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t channel_extent = 0;
    uint32_t slot_count = 0;
};

Status Errorf(StatusCode code, std::string_view prefix, uint64_t a,
              uint64_t b) {
    return Status::Error(code, std::string(prefix) + " (" +
                                   std::to_string(a) + " > " +
                                   std::to_string(b) + ")");
}

Status ValidateName(std::string_view name) {
    if (name.size() < 2 || name.front() != '/') {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must begin with '/' and a token");
    }
    if (name.find('/', 1) != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must not contain additional '/'");
    }
    return Status::Ok();
}

Status ValidateTopic(std::string_view topic) {
    if (topic.empty() || topic.size() > kMaxTopicName) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic name must be 1..63 bytes");
    }
    if (topic.find('\0') != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic name must not contain a NUL");
    }
    return Status::Ok();
}

bool TopicEquals(const TopicSlot& slot, std::string_view topic) {
    return std::strncmp(slot.name, topic.data(), topic.size()) == 0 &&
           slot.name[topic.size()] == '\0';
}

void WriteTopicName(TopicSlot* slot, std::string_view topic) {
    std::memset(slot->name, 0, sizeof(slot->name));
    std::memcpy(slot->name, topic.data(), topic.size());
}

uint64_t HostPageSize() {
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<uint64_t>(page) : 4096;
}

Result<SimpleNodeOptions> NormalizeOptions(SimpleNodeOptions options) {
    if (options.topic_slots == 0 || options.topic_slots > kMaxTopicSlots) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic_slots must be in [1, 16]");
    }
    if (options.queue_depth < kMinQueueDepth ||
        options.queue_depth > kMaxQueueDepth ||
        (options.queue_depth & (options.queue_depth - 1)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "queue_depth must be a power of two in [2, 1024]");
    }
    if (options.max_payload_bytes == 0 ||
        options.max_payload_bytes > kMaxPayloadBytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_payload_bytes must be in [1, 1 MiB]");
    }
    return options;
}

Result<uint64_t> AllocatorExtent(uint32_t slot_size, uint32_t slot_count) {
    ClassTableConfig config;
    config.classes = {{.slot_size = slot_size, .slot_count = slot_count}};
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));
    const uint32_t total_slots = table.total_slot_count();
    const uint32_t class_count = table.class_count();
    const uint32_t bitmap_words =
        (total_slots + kBitmapShardBits - 1) / kBitmapShardBits;

    uint64_t off = CentralSlabAllocator::kMetadataHeaderSize;
    uint64_t step = 0;
    if (!CheckedMulU64(sizeof(ClassDescriptor), class_count, &step) ||
        !CheckedAddU64(off, step, &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator metadata overflows");
    }
    if (!CheckedAlignUpU64(off, alignof(std::atomic<uint64_t>), &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator bitmap align overflows");
    }
    if (!CheckedMulU64(sizeof(std::atomic<uint64_t>), bitmap_words, &step) ||
        !CheckedAddU64(off, step, &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator bitmap overflows");
    }
    if (!CheckedAlignUpU64(off, alignof(std::atomic<uint32_t>), &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator generation align overflows");
    }
    if (!CheckedMulU64(sizeof(std::atomic<uint32_t>), total_slots, &step) ||
        !CheckedAddU64(off, step, &off) ||
        !CheckedMulU64(sizeof(std::atomic<uint32_t>), class_count, &step) ||
        !CheckedAddU64(off, step, &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator generation array overflows");
    }
    uint64_t stride = 0;
    if (!CheckedAddU64(sizeof(SlabHeader), table.max_object_size(), &stride) ||
        !CheckedAlignUpU64(stride, alignof(SlabHeader), &stride)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot stride overflows");
    }
    uint64_t metadata = 0;
    if (!CheckedAlignUpU64(off, alignof(SlabHeader), &metadata)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot area align overflows");
    }
    uint64_t slot_bytes = 0;
    if (!CheckedMulU64(stride, total_slots, &slot_bytes)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot bytes overflow");
    }
    uint64_t extent = 0;
    if (!CheckedAddU64(metadata, slot_bytes, &extent) ||
        !CheckedAlignUpU64(extent, kCacheLine, &extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator extent overflows");
    }
    return extent;
}

Result<SegmentLayout> ComputeLayout(const SimpleNodeOptions& options) {
    uint64_t in_flight = 0;
    if (!CheckedAddU64(options.queue_depth, 1, &in_flight) ||
        !CheckedMulU64(options.topic_slots, in_flight, &in_flight)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot count overflows");
    }
    if (in_flight > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot count exceeds uint32_t");
    }
    MINO_ASSIGN_OR_RETURN(
        uint64_t allocator_extent,
        AllocatorExtent(options.max_payload_bytes,
                        static_cast<uint32_t>(in_flight)));
    const uint64_t channel_extent =
        SpscChannel::RequiredSize(options.queue_depth);
    uint64_t channels_extent = 0;
    if (!CheckedMulU64(channel_extent, options.topic_slots, &channels_extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel area overflows");
    }

    SegmentLayout layout;
    layout.allocator_offset = sizeof(ManifestHeader);
    layout.allocator_extent = allocator_extent;
    uint64_t next = 0;
    if (!CheckedAddU64(layout.allocator_offset, layout.allocator_extent,
                       &next) ||
        !CheckedAlignUpU64(next, kCacheLine, &layout.channels_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel offset overflows");
    }
    layout.channel_extent = channel_extent;
    layout.channels_extent = channels_extent;
    if (!CheckedAddU64(layout.channels_offset, layout.channels_extent, &next) ||
        !CheckedAlignUpU64(next, HostPageSize(), &layout.total_size)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "segment size overflows");
    }
    layout.slot_count = static_cast<uint32_t>(in_flight);
    return layout;
}

Status CheckShmBudget(uint64_t data_bytes) {
    uint64_t needed = 0;
    if (!CheckedAddU64(data_bytes, kMarkerSlackBytes, &needed)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shared-memory size overflows");
    }

    struct statvfs vfs;
    if (::statvfs("/dev/shm", &vfs) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot query /dev/shm capacity");
    }
    const uint64_t block = vfs.f_frsize != 0 ? vfs.f_frsize : vfs.f_bsize;
    uint64_t available = 0;
    if (block != 0 &&
        vfs.f_bavail <= std::numeric_limits<uint64_t>::max() / block) {
        available = static_cast<uint64_t>(vfs.f_bavail) * block;
    }
    if (needed > available) {
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds free /dev/shm", needed,
                      available);
    }

    struct rlimit fsize;
    if (::getrlimit(RLIMIT_FSIZE, &fsize) == 0 &&
        fsize.rlim_cur != RLIM_INFINITY &&
        needed > static_cast<uint64_t>(fsize.rlim_cur)) {
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds RLIMIT_FSIZE", needed,
                      static_cast<uint64_t>(fsize.rlim_cur));
    }
    struct rlimit as;
    if (::getrlimit(RLIMIT_AS, &as) == 0 && as.rlim_cur != RLIM_INFINITY &&
        needed > static_cast<uint64_t>(as.rlim_cur)) {
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds RLIMIT_AS", needed,
                      static_cast<uint64_t>(as.rlim_cur));
    }
    return Status::Ok();
}

Status WaitForMagic(ManifestHeader* header, Deadline deadline) {
    while (header->magic.load(std::memory_order_acquire) != kMagic) {
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out waiting for SimpleNode Create");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return Status::Ok();
}

Status ValidateHeader(const SharedMemorySegment& segment,
                      const ManifestHeader& header) {
    if (header.version != kVersion ||
        header.header_size != sizeof(ManifestHeader)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "SimpleNode manifest version/size mismatch");
    }
    if (header.reserved0 != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode reserved field is nonzero");
    }
    if (header.total_size != segment.size()) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode total-size mismatch");
    }
    if (header.topic_slots == 0 || header.topic_slots > kMaxTopicSlots ||
        header.queue_depth < kMinQueueDepth ||
        (header.queue_depth & (header.queue_depth - 1)) != 0 ||
        header.max_payload_bytes == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode layout fields are invalid");
    }
    uint64_t alloc_end = 0;
    uint64_t chan_end = 0;
    if (header.allocator_offset % kCacheLine != 0 ||
        header.allocator_extent % kCacheLine != 0 ||
        header.channels_offset % kCacheLine != 0 ||
        !CheckedAddU64(header.allocator_offset, header.allocator_extent,
                       &alloc_end) ||
        !CheckedAddU64(header.channels_offset, header.channels_extent,
                       &chan_end) ||
        alloc_end > header.total_size || chan_end > header.total_size) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode extents are invalid");
    }
    return Status::Ok();
}

std::byte* BytesOf(SharedMemorySegment& segment) {
    return static_cast<std::byte*>(segment.base());
}

}  // namespace

struct BorrowedBytes::Impl {
    CentralSlabAllocator* allocator = nullptr;
    SpscChannel::Borrow borrow;
    const std::byte* data = nullptr;
    uint32_t size = 0;
    ShmHandle handle;
    bool active = false;
};

BorrowedBytes::BorrowedBytes() noexcept = default;
BorrowedBytes::BorrowedBytes(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
BorrowedBytes::BorrowedBytes(BorrowedBytes&& other) noexcept = default;
BorrowedBytes& BorrowedBytes::operator=(BorrowedBytes&& other) noexcept =
    default;
BorrowedBytes::~BorrowedBytes() {
    if (impl_ && impl_->active) {
        (void)std::move(*this).Release();
    }
}

std::span<const std::byte> BorrowedBytes::bytes() const noexcept {
    if (impl_ == nullptr || !impl_->active || impl_->data == nullptr) {
        return {};
    }
    return {impl_->data, impl_->size};
}

bool BorrowedBytes::active() const noexcept {
    return impl_ != nullptr && impl_->active;
}

Status BorrowedBytes::Release() && noexcept {
    if (impl_ == nullptr || !impl_->active) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "borrowed bytes are not active");
    }
    impl_->active = false;
    const Status ack = std::move(impl_->borrow).Ack();
    Status reclaim = Status::Ok();
    if (ack.ok() && impl_->allocator != nullptr && !impl_->handle.IsNull()) {
        reclaim = impl_->allocator->Retire(impl_->handle);
        if (reclaim.ok()) {
            reclaim = impl_->allocator->Reclaim(impl_->handle);
        }
    }
    impl_->data = nullptr;
    impl_->allocator = nullptr;
    if (!ack.ok()) return ack;
    return reclaim;
}

struct SimplePublisher::Impl {
    CentralSlabAllocator* allocator = nullptr;
    SpscChannel channel;
    uint32_t max_payload_bytes = 0;
};

SimplePublisher::SimplePublisher(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimplePublisher::SimplePublisher(SimplePublisher&& other) noexcept = default;
SimplePublisher& SimplePublisher::operator=(SimplePublisher&& other) noexcept =
    default;
SimplePublisher::~SimplePublisher() = default;

Status SimplePublisher::Publish(std::span<const std::byte> payload,
                                Deadline deadline) {
    if (impl_ == nullptr || impl_->allocator == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "publisher is not bound");
    }
    if (payload.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload must not be empty");
    }
    if (payload.size() > impl_->max_payload_bytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload exceeds max_payload_bytes");
    }
    if (deadline.expired()) {
        return Status::Error(StatusCode::kTimeout,
                             "publish deadline expired");
    }

    AllocationRequest request;
    request.object_size = static_cast<uint32_t>(payload.size());
    request.type_id = TypeId{kSimpleMsgType};
    request.schema = SchemaIdentity{
        .short_id = kSimpleSchemaShortId,
        .layout_version = kSimpleLayoutVersion,
    };
    request.alignment = 1;
    MINO_ASSIGN_OR_RETURN(ShmHandle handle,
                          impl_->allocator->Allocate(request));
    Result<MutableBuildView> build = impl_->allocator->BeginBuild(handle);
    if (!build.ok()) {
        (void)impl_->allocator->Abort(handle);
        return build.status();
    }
    if (build->data == nullptr || build->object_size != payload.size() ||
        build->capacity < payload.size()) {
        (void)impl_->allocator->Abort(handle);
        return Status::Error(StatusCode::kCorruption,
                             "allocator returned an invalid bytes view");
    }
    std::memcpy(build->data, payload.data(), payload.size());
    Status published = impl_->allocator->Publish(handle);
    if (!published.ok()) {
        (void)impl_->allocator->Abort(handle);
        return published;
    }

    for (;;) {
        Result<SpscChannel::Reservation> reserved =
            impl_->channel.Reserve(QueueFullPolicy::kFail);
        if (reserved.ok()) {
            reserved.value()->msg_type = kSimpleMsgType;
            reserved.value()->schema_version = kSimpleSchemaVersion;
            reserved.value()->schema_short_id = kSimpleSchemaShortId;
            reserved.value()->schema_layout_version = kSimpleLayoutVersion;
            reserved.value()->reserved0 = 0;
            reserved.value()->timestamp_ns = 0;
            reserved.value()->payload = handle;
            reserved.value()->payload_len =
                static_cast<uint32_t>(payload.size());
            reserved.value()->flags = 0;
            const Status commit = std::move(reserved.value()).Commit();
            if (!commit.ok()) {
                (void)impl_->allocator->Retire(handle);
                (void)impl_->allocator->Reclaim(handle);
            }
            return commit;
        }
        if (reserved.status().code() != StatusCode::kResourceExhausted) {
            (void)impl_->allocator->Retire(handle);
            (void)impl_->allocator->Reclaim(handle);
            return reserved.status();
        }
        if (deadline.expired()) {
            (void)impl_->allocator->Retire(handle);
            (void)impl_->allocator->Reclaim(handle);
            return Status::Error(StatusCode::kTimeout,
                                 "publish deadline expired");
        }
        std::this_thread::yield();
    }
}

struct SimpleSubscriber::Impl {
    CentralSlabAllocator* allocator = nullptr;
    SpscChannel channel;
};

SimpleSubscriber::SimpleSubscriber(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimpleSubscriber::SimpleSubscriber(SimpleSubscriber&& other) noexcept =
    default;
SimpleSubscriber& SimpleSubscriber::operator=(
    SimpleSubscriber&& other) noexcept = default;
SimpleSubscriber::~SimpleSubscriber() = default;

Result<BorrowedBytes> SimpleSubscriber::TryPoll() {
    if (impl_ == nullptr || impl_->allocator == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "subscriber is not bound");
    }
    MINO_ASSIGN_OR_RETURN(SpscChannel::Borrow borrow, impl_->channel.Poll());
    const IndexSlotSnapshot& slot = *borrow.slot();
    if (slot.msg_type != kSimpleMsgType ||
        slot.schema_version != kSimpleSchemaVersion ||
        slot.schema_short_id != kSimpleSchemaShortId ||
        slot.schema_layout_version != kSimpleLayoutVersion) {
        (void)std::move(borrow).Ack();
        return Status::Error(StatusCode::kSchemaMismatch,
                             "payload identity does not match SimpleNode bytes");
    }
    Result<SlabView> view = impl_->allocator->Inspect(slot.payload);
    if (!view.ok()) {
        (void)std::move(borrow).Ack();
        return view.status();
    }
    if (view->state != ObjectState::kPublished || view->data == nullptr ||
        view->object_size != slot.payload_len ||
        view->capacity < slot.payload_len) {
        (void)std::move(borrow).Ack();
        return Status::Error(StatusCode::kSchemaMismatch,
                             "payload slab does not match published bytes");
    }
    auto borrowed = std::unique_ptr<BorrowedBytes::Impl>(new BorrowedBytes::Impl());
    borrowed->allocator = impl_->allocator;
    borrowed->borrow = std::move(borrow);
    borrowed->data = static_cast<const std::byte*>(view->data);
    borrowed->size = slot.payload_len;
    borrowed->handle = slot.payload;
    borrowed->active = true;
    return BorrowedBytes(std::move(borrowed));
}

Result<BorrowedBytes> SimpleSubscriber::Poll(Deadline deadline) {
    for (;;) {
        Result<BorrowedBytes> message = TryPoll();
        if (message.ok()) return message;
        if (message.status().code() != StatusCode::kWouldBlock) {
            return message.status();
        }
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "subscriber poll deadline expired");
        }
        std::this_thread::yield();
    }
}

struct SimpleNode::Impl {
    std::optional<SharedMemorySegment> segment;
    CentralSlabAllocator allocator;
    ManifestHeader* header = nullptr;
    std::string name;
};

SimpleNode::SimpleNode(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimpleNode::SimpleNode(SimpleNode&& other) noexcept = default;
SimpleNode& SimpleNode::operator=(SimpleNode&& other) noexcept = default;
SimpleNode::~SimpleNode() = default;

Result<uint64_t> SimpleNode::RequiredBytes(const SimpleNodeOptions& options) {
    MINO_ASSIGN_OR_RETURN(SimpleNodeOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));
    if (normalized.segment_bytes != 0) {
        if (normalized.segment_bytes < layout.total_size) {
            return Errorf(StatusCode::kInvalidArgument,
                          "segment_bytes is smaller than RequiredBytes",
                          layout.total_size, normalized.segment_bytes);
        }
        uint64_t rounded = 0;
        if (!CheckedAlignUpU64(normalized.segment_bytes, HostPageSize(),
                               &rounded)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "segment_bytes alignment overflows");
        }
        return rounded;
    }
    return layout.total_size;
}

Result<SimpleNode> SimpleNode::Create(std::string_view name,
                                      SimpleNodeOptions options) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    MINO_ASSIGN_OR_RETURN(SimpleNodeOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(uint64_t bytes, RequiredBytes(normalized));
    MINO_RETURN_IF_ERROR(CheckShmBudget(bytes));

    SharedMemoryCreateOptions create;
    create.name = std::string(name);
    create.size = bytes;
    create.use_huge_pages = false;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Create(create));
    std::memset(segment.base(), 0, static_cast<size_t>(segment.size()));

    auto* header = std::construct_at(
        static_cast<ManifestHeader*>(segment.base()));
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));
    header->version = kVersion;
    header->header_size = sizeof(ManifestHeader);
    header->total_size = segment.size();
    header->topic_slots = normalized.topic_slots;
    header->queue_depth = normalized.queue_depth;
    header->max_payload_bytes = normalized.max_payload_bytes;
    header->allocator_offset = layout.allocator_offset;
    header->allocator_extent = layout.allocator_extent;
    header->channels_offset = layout.channels_offset;
    header->channels_extent = layout.channels_extent;

    std::byte* base = BytesOf(segment);
    ClassTableConfig allocator_config;
    allocator_config.classes = {
        {.slot_size = normalized.max_payload_bytes,
         .slot_count = layout.slot_count},
    };
    Result<CentralSlabAllocator> allocator = CentralSlabAllocator::Create(
        base + layout.allocator_offset, layout.allocator_extent,
        allocator_config);
    if (!allocator.ok()) return allocator.status();

    for (uint32_t i = 0; i < normalized.topic_slots; ++i) {
        uint64_t offset = 0;
        if (!CheckedMulU64(layout.channel_extent, i, &offset) ||
            !CheckedAddU64(layout.channels_offset, offset, &offset)) {
            return Status::Error(StatusCode::kInternal,
                                 "channel offset overflows");
        }
        TopicSlot& slot = header->topics[i];
        slot.channel_offset = offset;
        slot.channel_extent = layout.channel_extent;
        slot.capacity = normalized.queue_depth;
        Result<SpscChannel> channel =
            SpscChannel::Init(base + offset, normalized.queue_depth);
        if (!channel.ok()) return channel.status();
    }

    header->magic.store(kMagic, std::memory_order_release);

    auto impl = std::unique_ptr<Impl>(new Impl());
    impl->segment = std::move(segment);
    impl->allocator = std::move(*allocator);
    impl->header = header;
    impl->name = std::string(name);
    return SimpleNode(std::move(impl));
}

Result<SimpleNode> SimpleNode::Open(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    SharedMemoryOpenOptions open;
    open.name = std::string(name);
    open.read_only = false;
    open.creating_wait_timeout_ms = 2000;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Open(open));
    if (segment.base() == nullptr ||
        segment.size() < sizeof(ManifestHeader) ||
        reinterpret_cast<uintptr_t>(segment.base()) % kCacheLine != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "shared-memory mapping cannot hold SimpleNode");
    }
    auto* header = static_cast<ManifestHeader*>(segment.base());
    MINO_RETURN_IF_ERROR(
        WaitForMagic(header, Deadline::FromNow(std::chrono::seconds(2))));
    MINO_RETURN_IF_ERROR(ValidateHeader(segment, *header));

    std::byte* base = BytesOf(segment);
    MINO_ASSIGN_OR_RETURN(
        CentralSlabAllocator allocator,
        CentralSlabAllocator::Attach(base + header->allocator_offset,
                                     header->allocator_extent));
    auto impl = std::unique_ptr<Impl>(new Impl());
    impl->segment = std::move(segment);
    impl->allocator = std::move(allocator);
    impl->header = header;
    impl->name = std::string(name);
    return SimpleNode(std::move(impl));
}

Status SimpleNode::Unlink(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    return SharedMemorySegment::Unlink(std::string(name));
}

Result<SimplePublisher> SimpleNode::Advertise(std::string_view topic) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    if (impl_ == nullptr || impl_->header == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode is not bound");
    }
    ManifestHeader* header = impl_->header;
    TopicSlot* claimed = nullptr;
    for (int attempt = 0; attempt < 64; ++attempt) {
        claimed = nullptr;
        bool waiting = false;
        for (uint32_t i = 0; i < header->topic_slots; ++i) {
            TopicSlot& slot = header->topics[i];
            const uint32_t state = slot.state.load(std::memory_order_acquire);
            if (state == kTopicEmpty) continue;
            if (TopicEquals(slot, topic)) {
                if (state == kTopicReady) {
                    return Status::Error(StatusCode::kAlreadyExists,
                                         "topic is already advertised");
                }
                waiting = true;
            }
        }
        if (waiting) {
            std::this_thread::yield();
            continue;
        }
        for (uint32_t i = 0; i < header->topic_slots; ++i) {
            TopicSlot& slot = header->topics[i];
            uint32_t expected = kTopicEmpty;
            if (slot.state.compare_exchange_strong(
                    expected, kTopicClaiming, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                bool duplicate = false;
                for (uint32_t j = 0; j < header->topic_slots; ++j) {
                    if (j == i) continue;
                    TopicSlot& other = header->topics[j];
                    const uint32_t other_state =
                        other.state.load(std::memory_order_acquire);
                    if (other_state != kTopicEmpty &&
                        TopicEquals(other, topic)) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) {
                    slot.state.store(kTopicEmpty, std::memory_order_release);
                    return Status::Error(StatusCode::kAlreadyExists,
                                         "topic is already advertised");
                }
                WriteTopicName(&slot, topic);
                slot.subscriber_claimed.store(0, std::memory_order_relaxed);
                slot.state.store(kTopicReady, std::memory_order_release);
                claimed = &slot;
                break;
            }
        }
        if (claimed != nullptr) break;
        if (attempt == 63) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic directory is full");
        }
        std::this_thread::yield();
    }

    std::byte* base = BytesOf(*impl_->segment);
    MINO_ASSIGN_OR_RETURN(SpscChannel channel,
                          SpscChannel::Attach(base + claimed->channel_offset));
    if (channel.capacity() != claimed->capacity) {
        return Status::Error(StatusCode::kCorruption,
                             "attached channel capacity mismatch");
    }
    auto pub = std::unique_ptr<SimplePublisher::Impl>(new SimplePublisher::Impl());
    pub->allocator = &impl_->allocator;
    pub->channel = channel;
    pub->max_payload_bytes = header->max_payload_bytes;
    return SimplePublisher(std::move(pub));
}

Result<SimpleSubscriber> SimpleNode::Subscribe(std::string_view topic,
                                               Deadline deadline) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    if (impl_ == nullptr || impl_->header == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode is not bound");
    }
    ManifestHeader* header = impl_->header;
    TopicSlot* found = nullptr;
    for (;;) {
        for (uint32_t i = 0; i < header->topic_slots; ++i) {
            TopicSlot& slot = header->topics[i];
            if (slot.state.load(std::memory_order_acquire) == kTopicReady &&
                TopicEquals(slot, topic)) {
                found = &slot;
                break;
            }
        }
        if (found != nullptr) break;
        if (deadline.expired()) {
            return Status::Error(StatusCode::kNotFound,
                                 "topic is not advertised");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    uint32_t expected = 0;
    if (!found->subscriber_claimed.compare_exchange_strong(
            expected, 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "topic already has a subscriber");
    }
    std::byte* base = BytesOf(*impl_->segment);
    MINO_ASSIGN_OR_RETURN(SpscChannel channel,
                          SpscChannel::Attach(base + found->channel_offset));
    auto sub = std::unique_ptr<SimpleSubscriber::Impl>(new SimpleSubscriber::Impl());
    sub->allocator = &impl_->allocator;
    sub->channel = channel;
    return SimpleSubscriber(std::move(sub));
}

uint64_t SimpleNode::size_bytes() const noexcept {
    return impl_ == nullptr ? 0 : impl_->segment->size();
}

const std::string& SimpleNode::name() const noexcept {
    static const std::string kEmpty;
    return impl_ == nullptr ? kEmpty : impl_->name;
}

}  // namespace mino
