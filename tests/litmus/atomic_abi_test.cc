// Copyright (c) 2026 Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only
//
// V-12: Linux x86-64 原子 ABI Litmus Test（ADR-0001，详设 26 章验证登记表）
//
// 验证目标：
//   1. std::atomic<uint64_t/uint32_t/uint16_t/bool> 为 lock-free；
//   2. shm_open + mmap 共享内存映射上 placement-new 的 std::atomic<uint64_t>
//      fetch_add / compare_exchange 行为正确；
//   3. 报告 128-bit 原子能力；v1 SHM ABI 不依赖 128-bit 原子，非 lock-free 时禁止使用；
//   4. 跨进程（fork 共享同一 shm 映射）交替原子操作的 sequential consistency。
//
// 平台策略：v1 必需能力不可用时 FAIL；可选 128-bit 能力只报告并禁止进入 SHM ABI。

#include <gtest/gtest.h>

#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#define MINO_HAS_POSIX 1
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#else
#define MINO_HAS_POSIX 0
#endif

namespace mino {
namespace litmus {
namespace {

// ---------------------------------------------------------------------------
// 平台能力检测
// ---------------------------------------------------------------------------

#if defined(__x86_64__) || defined(_M_X64)
#define MINO_ARCH_X86_64 1
#else
#define MINO_ARCH_X86_64 0
#endif

// GCC/Clang 在启用 cx16（-mcx16）时定义 __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16
#if defined(__SIZEOF_INT128__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
#define MINO_HAS_ATOMIC_I128 1
#else
#define MINO_HAS_ATOMIC_I128 0
#endif

// ---------------------------------------------------------------------------
// POSIX 共享内存 RAII 封装
// ---------------------------------------------------------------------------

#if MINO_HAS_POSIX

// 共享布局：父进程在 fork 前创建并初始化，子进程继承映射。
// cache line 隔离避免控制字段与数据字段互相干扰。
struct SharedLayout {
  alignas(64) std::atomic<uint64_t> counter;
  alignas(64) std::atomic<uint64_t> slot;
  alignas(64) std::atomic<uint64_t> turn;
  alignas(64) std::atomic<uint64_t> handshake;
  alignas(64) std::atomic<uint64_t> child_result;  // 0=运行中 1=通过 2=失败
};

// 固定 shm 名称 + 环境变量传 fd，规避 Bazel sandbox 与 macOS 上
// 无名 memfd 无法跨 fork 传递的限制。
constexpr const char* kShmName = "/mino_v12_litmus";
constexpr const char* kFdEnvVar = "MINO_LITMUS_SHM_FD";

// 打开（必要时创建）指定大小的 POSIX shm 对象。
// 若调用时对象已存在且调用者声明了 created==nullptr，则按已有对象打开。
int OpenOrCreateShm(bool create, size_t size, bool* created) {
  int flags = O_RDWR;
  if (create) flags |= O_CREAT | O_EXCL;
  int fd = ::shm_open(kShmName, flags, 0600);
  if (fd < 0 && create && errno == EEXIST) {
    // 上次异常退出残留的 shm 对象：先清理再重建
    ::shm_unlink(kShmName);
    fd = ::shm_open(kShmName, flags, 0600);
  }
  if (fd < 0) return -1;
  if (create) {
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
      int e = errno;
      ::close(fd);
      ::shm_unlink(kShmName);
      errno = e;
      return -1;
    }
    if (created) *created = true;
  } else if (created) {
    *created = false;
  }
  return fd;
}

class ShmRegion {
 public:
  ~ShmRegion() {
    if (map_ != nullptr && map_ != MAP_FAILED) ::munmap(map_, size_);
    if (fd_ >= 0) ::close(fd_);
    if (owner_) ::shm_unlink(kShmName);
  }

  ShmRegion(const ShmRegion&) = delete;
  ShmRegion& operator=(const ShmRegion&) = delete;

  // create=true 时创建（unlink 旧对象、ftruncate、清零初始化），
  // create=false 时按已有对象 attach（子进程通过 fd 继承路径不使用此函数）。
  static std::unique_ptr<ShmRegion> Open(size_t size, bool create) {
    auto region = std::unique_ptr<ShmRegion>(new ShmRegion());
    region->size_ = size;
    bool created = false;
    region->fd_ = OpenOrCreateShm(create, size, &created);
    if (region->fd_ < 0) {
      region->error_ = std::string("shm_open: ") + std::strerror(errno);
      return region;
    }
    region->owner_ = create;
    region->map_ =
        ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, region->fd_, 0);
    if (region->map_ == MAP_FAILED) {
      region->error_ = std::string("mmap: ") + std::strerror(errno);
      region->map_ = nullptr;
      return region;
    }
    return region;
  }

  // 从继承的 fd attach（fork 子进程路径）
  static std::unique_ptr<ShmRegion> AttachFd(int fd, size_t size) {
    auto region = std::unique_ptr<ShmRegion>(new ShmRegion());
    region->size_ = size;
    region->fd_ = fd;
    region->map_ =
        ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (region->map_ == MAP_FAILED) {
      region->error_ = std::string("mmap: ") + std::strerror(errno);
      region->map_ = nullptr;
      return region;
    }
    return region;
  }

  bool ok() const { return map_ != nullptr; }
  const std::string& error() const { return error_; }
  void* data() const { return map_; }
  int fd() const { return fd_; }

 private:
  ShmRegion() = default;
  int fd_ = -1;
  void* map_ = nullptr;
  size_t size_ = 0;
  bool owner_ = false;
  std::string error_;
};

#endif  // MINO_HAS_POSIX

// ---------------------------------------------------------------------------
// V-12-1: 内建宽度原子类型的 lock-free 断言
// ---------------------------------------------------------------------------

TEST(AtomicAbiLockFreeTest, BuiltinWidthsAreLockFree) {
  // ATOMIC_*_LOCK_FREE == 2 表示“always lock-free”（编译期保证）
  EXPECT_EQ(ATOMIC_BOOL_LOCK_FREE, 2) << "std::atomic<bool> 非 always lock-free";
  EXPECT_EQ(ATOMIC_CHAR16_T_LOCK_FREE, 2)
      << "std::atomic<char16_t> (16-bit) 非 always lock-free";
  EXPECT_EQ(ATOMIC_INT_LOCK_FREE, 2)
      << "std::atomic<int> (32-bit) 非 always lock-free";
  EXPECT_EQ(ATOMIC_LLONG_LOCK_FREE, 2)
      << "std::atomic<long long> (64-bit) 非 always lock-free";

  // 运行时 is_lock_free() 复核
  std::atomic<uint64_t> a64{0};
  std::atomic<uint32_t> a32{0};
  std::atomic<uint16_t> a16{0};
  std::atomic<bool> ab{false};

  EXPECT_TRUE(a64.is_lock_free()) << "std::atomic<uint64_t> 非 lock-free";
  EXPECT_TRUE(a32.is_lock_free()) << "std::atomic<uint32_t> 非 lock-free";
  EXPECT_TRUE(a16.is_lock_free()) << "std::atomic<uint16_t> 非 lock-free";
  EXPECT_TRUE(ab.is_lock_free()) << "std::atomic<bool> 非 lock-free";

  // C++17 起所有特化提供静态 constexpr 查询
  static_assert(std::atomic<uint64_t>::is_always_lock_free,
                "uint64_t 原子在目标平台非 always lock-free");
  static_assert(std::atomic<uint32_t>::is_always_lock_free,
                "uint32_t 原子在目标平台非 always lock-free");
  static_assert(std::atomic<uint16_t>::is_always_lock_free,
                "uint16_t 原子在目标平台非 always lock-free");
  static_assert(std::atomic<bool>::is_always_lock_free,
                "bool 原子在目标平台非 always lock-free");

  std::printf("[V-12] lock-free: u64=%d u32=%d u16=%d bool=%d\n",
              static_cast<int>(a64.is_lock_free()),
              static_cast<int>(a32.is_lock_free()),
              static_cast<int>(a16.is_lock_free()),
              static_cast<int>(ab.is_lock_free()));
}

// ---------------------------------------------------------------------------
// V-12-2: 128-bit 原子（__int128）lock-free 验证
// ---------------------------------------------------------------------------

TEST(AtomicAbiLockFreeTest, AtomicInt128CapabilityIsReported) {
#if defined(__SIZEOF_INT128__)
#if MINO_HAS_ATOMIC_I128
  // V-12 的本职是「验证并报告」lock-free 能力，而不是让构建失败：
  // 在 TSAN/ASAN 下 libstdc++ 把 128 位原子 lower 为运行时调用
  //（__tsan_atomic* / libatomic），is_always_lock_free 编译期为 false。
  // 注意：即便 lock-free，libstdc++ 对 128 位原子也不提供 fetch_add 等算术
  // RMW（只为 ≤64 位整型特化提供），128 位只支持 load/store/CAS/exchange。
  // 因此这里用 if constexpr 在编译期分派：仅 lock-free 工具链（debug/
  // release + cx16）才实例化 std::atomic<u128> 并验证 CAS；非 lock-free
  // 路径根本不声明该对象，只做报告与 SKIP。
  if constexpr (std::atomic<unsigned __int128>::is_always_lock_free) {
    using u128 = unsigned __int128;
    std::atomic<u128> value{0};
    // 128 位原子无 fetch_add，用 CAS 完成等效的原子更新验证。每次 CAS 前
    // 显式重置 expected：CAS 无论成败都会覆写 expected，跨调用复用同一
    // 变量会引入工具链相关的歧义。
    u128 expected = 0;
    ASSERT_TRUE(value.compare_exchange_strong(expected, static_cast<u128>(1),
                                              std::memory_order_seq_cst));
    EXPECT_EQ(value.load(std::memory_order_seq_cst), static_cast<u128>(1));
    const u128 desired = (static_cast<u128>(1) << 64) | 0x2a;
    expected = 1;
    EXPECT_TRUE(value.compare_exchange_strong(expected, desired,
                                              std::memory_order_seq_cst));
    EXPECT_EQ(value.load(std::memory_order_seq_cst), desired);
    std::printf("[V-12] 128-bit atomic: lock-free=1 (cx16)\n");
  } else {
    std::printf("[V-12] 128-bit atomic: lock-free=0 (runtime-lowered, e.g. "
                "sanitizer/libatomic)\n");
    std::printf("[V-12] 128-bit atomic is optional for v1; "
                "non-lock-free capability is prohibited from the SHM ABI\n");
  }
#else
  std::printf("[V-12] optional 128-bit atomic unavailable: compiler has no "
              "16-byte CAS support; prohibited from the v1 SHM ABI\n");
#endif
#else
  std::printf("[V-12] optional 128-bit atomic unavailable: no __int128; "
              "prohibited from the v1 SHM ABI\n");
#endif
}

// ---------------------------------------------------------------------------
// V-12-3: 共享内存映射上的 fetch_add / compare_exchange
// ---------------------------------------------------------------------------

TEST(AtomicAbiSharedMemoryTest, FetchAddAndCasOnShmMapping) {
#if !MINO_HAS_POSIX
  GTEST_SKIP() << "SKIP: 非 POSIX 平台，无 shm_open/mmap";
#else
  auto region = ShmRegion::Open(sizeof(SharedLayout), /*create=*/true);
  if (!region->ok()) {
    GTEST_SKIP() << "SKIP: 共享内存不可用: " << region->error();
  }

  auto* layout = new (region->data()) SharedLayout();
  layout->counter.store(0, std::memory_order_relaxed);
  layout->slot.store(0, std::memory_order_relaxed);

  // fetch_add 累加
  constexpr uint64_t kIterations = 100000;
  for (uint64_t i = 0; i < kIterations; ++i) {
    layout->counter.fetch_add(1, std::memory_order_relaxed);
  }
  EXPECT_EQ(layout->counter.load(std::memory_order_relaxed), kIterations);

  // compare_exchange：成功路径
  uint64_t expected = kIterations;
  ASSERT_TRUE(layout->counter.compare_exchange_strong(
      expected, 7, std::memory_order_seq_cst, std::memory_order_seq_cst));
  EXPECT_EQ(layout->counter.load(std::memory_order_seq_cst), 7u);

  // compare_exchange：失败路径（expected 被刷新为当前值）
  expected = 999;
  ASSERT_FALSE(layout->counter.compare_exchange_strong(
      expected, 1, std::memory_order_seq_cst, std::memory_order_seq_cst));
  EXPECT_EQ(expected, 7u);
  EXPECT_EQ(layout->counter.load(std::memory_order_seq_cst), 7u);
#endif
}

// ---------------------------------------------------------------------------
// V-12-4: 跨进程 Litmus —— fork 后父子进程交替 RMW，验证顺序一致性
// ---------------------------------------------------------------------------
//
// 协议（turn-based 严格交替，双方均为 seq_cst）：
//   turn == 0 → 父进程可行动；turn == 1 → 子进程可行动；turn == 2 → 终局。
//   父：loop i in [1, N]: counter.fetch_add(i) → slot 同步 → turn=1 → 等待子回棒
//   子：loop i in [1, N]: 等待 turn==1 → 校验 counter 与 slot 一致 →
//        counter.fetch_add(i) → slot 同步 → turn=0（i==N 时 turn=2 结束）
//
// 每轮父子各加 i，N 轮后 counter == 2 * (1+..+N) == N*(N+1)。
// 每次操作后立即将 counter 快照写入 slot，对方在下一次醒来时校验
// slot == counter == expected：任何丢失更新或乱序可见都会破坏该不变量。

TEST(AtomicAbiSharedMemoryTest, CrossProcessSequentiallyConsistent) {
#if !MINO_HAS_POSIX
  GTEST_SKIP() << "SKIP: 非 POSIX 平台，无 fork/shm_open";
#else
  auto region = ShmRegion::Open(sizeof(SharedLayout), /*create=*/true);
  if (!region->ok()) {
    GTEST_SKIP() << "SKIP: 共享内存不可用: " << region->error();
  }

  auto* layout = new (region->data()) SharedLayout();
  layout->counter.store(0, std::memory_order_relaxed);
  layout->slot.store(0, std::memory_order_relaxed);
  layout->turn.store(0, std::memory_order_relaxed);
  layout->handshake.store(0, std::memory_order_relaxed);
  layout->child_result.store(0, std::memory_order_relaxed);

  // 子进程通过环境变量拿到已就绪的 shm fd（规避 Bazel sandbox / macOS
  // shm 命名空间差异）。fd 标记为 inheritable 以穿透 fork。
  int flags = ::fcntl(region->fd(), F_GETFD);
  ASSERT_NE(flags, -1);
  ASSERT_EQ(::fcntl(region->fd(), F_SETFD, flags & ~FD_CLOEXEC), 0);
  char fd_buf[16];
  std::snprintf(fd_buf, sizeof(fd_buf), "%d", region->fd());
  ASSERT_EQ(::setenv(kFdEnvVar, fd_buf, /*overwrite=*/1), 0);

  constexpr uint64_t kRounds = 100000;

  pid_t pid = ::fork();
  if (pid < 0) {
    ::unsetenv(kFdEnvVar);
    GTEST_SKIP() << "SKIP: fork 失败: " << std::strerror(errno);
  }

  if (pid == 0) {
    // ---------------- 子进程 ----------------
    // 重新按 fd attach（独立映射，基址可与父进程不同）
    const char* fd_env = std::getenv(kFdEnvVar);
    if (fd_env == nullptr) _exit(100);
    int inherited_fd = std::atoi(fd_env);
    auto child_region = ShmRegion::AttachFd(inherited_fd, sizeof(SharedLayout));
    if (!child_region->ok()) _exit(101);
    auto* c = static_cast<SharedLayout*>(child_region->data());

    uint64_t expected_total = 0;
    for (uint64_t i = 1; i <= kRounds; ++i) {
      // 等待父进程完成第 i 轮
      while (c->turn.load(std::memory_order_seq_cst) != 1) {
      }
      const uint64_t slot_snap = c->slot.load(std::memory_order_seq_cst);
      const uint64_t counter_snap = c->counter.load(std::memory_order_seq_cst);
      expected_total += i;  // 父进程本轮的贡献
      if (slot_snap != counter_snap || counter_snap != expected_total) {
        std::fprintf(stderr,
                     "[V-12] 子进程观测到乱序: round=%" PRIu64
                     " slot=%" PRIu64 " counter=%" PRIu64 " expected=%" PRIu64 "\n",
                     i, slot_snap, counter_snap, expected_total);
        c->child_result.store(2, std::memory_order_seq_cst);
        c->turn.store(2, std::memory_order_seq_cst);
        _exit(1);
      }
      c->counter.fetch_add(i, std::memory_order_seq_cst);
      expected_total += i;
      c->slot.store(expected_total, std::memory_order_seq_cst);
      // 最后一轮置 turn=2 通知父进程收尾
      c->turn.store(i == kRounds ? 2 : 0, std::memory_order_seq_cst);
    }
    c->child_result.store(1, std::memory_order_seq_cst);
    _exit(0);
  }

  // ---------------- 父进程 ----------------
  // 父子严格交替：父先发（turn=0 语义为父可行动，由父的写启动每轮）。
  // 每轮父先加 i、同步 slot、交棒（turn=1）；子验证后加 i、同步 slot、
  // 回棒（turn=0；最后一轮置 turn=2 终局）。父在 i<N 轮回棒后校验。
  uint64_t expected_total = 0;
  for (uint64_t i = 1; i <= kRounds; ++i) {
    layout->counter.fetch_add(i, std::memory_order_seq_cst);
    expected_total += i;
    layout->slot.store(expected_total, std::memory_order_seq_cst);
    layout->turn.store(1, std::memory_order_seq_cst);
    // 等待子进程完成第 i 轮（子进程完成后置 0；最后一轮置 2）
    // 若子进程异常退出（校验失败 _exit(1)），避免父进程永久自旋
    uint64_t t;
    while ((t = layout->turn.load(std::memory_order_seq_cst)) == 1) {
      int early_status = 0;
      if (::waitpid(pid, &early_status, WNOHANG) == pid) {
        ::unsetenv(kFdEnvVar);
        FAIL() << "子进程在 round=" << i << " 异常退出"
               << (WIFEXITED(early_status)
                       ? (std::string(", exitcode=") +
                          std::to_string(WEXITSTATUS(early_status)))
                       : ", killed by signal");
      }
    }
    if (i < kRounds) {
      ASSERT_EQ(t, 0u) << "子进程在 round=" << i << " 提前终止";
      expected_total += i;  // 子进程本轮的贡献
      const uint64_t slot_snap = layout->slot.load(std::memory_order_seq_cst);
      const uint64_t counter_snap =
          layout->counter.load(std::memory_order_seq_cst);
      ASSERT_EQ(slot_snap, counter_snap)
          << "父进程观测到乱序: round=" << i;
      ASSERT_EQ(counter_snap, expected_total)
          << "父进程观测到丢失更新: round=" << i;
    } else {
      ASSERT_EQ(t, 2u) << "子进程未正常完成最后一轮";
    }
  }

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ::unsetenv(kFdEnvVar);

  ASSERT_TRUE(WIFEXITED(status)) << "子进程异常终止, status=" << status;
  ASSERT_EQ(WEXITSTATUS(status), 0) << "子进程校验失败";
  EXPECT_EQ(layout->child_result.load(std::memory_order_seq_cst), 1u);
  // 每轮父子各加 i，N 轮总和 = 2 * (1+..+N) = N*(N+1)
  EXPECT_EQ(layout->counter.load(std::memory_order_seq_cst),
            kRounds * (kRounds + 1))
      << "跨进程 fetch_add 总数不守恒";
#endif
}

}  // namespace
}  // namespace litmus
}  // namespace mino
