# V-12 Litmus Test：Linux x86-64 原子 ABI 基线

> 验证登记：**V-12**（详设第 26 章，优先级 P0）
> 关联决策：**ADR-0001**（首版平台基线：共享原子必须 Lock-free，并通过跨进程 Litmus Test）
> 工作项：开发计划 **D0-15**（Linux x86-64 原子 ABI 验证）
> 下游依赖：V-26（通用 MPMC 骨架）依赖本验证的跨进程原子语义结论

## 1. 测试目的

Mino 的 Index RingBuffer、Slab Allocator Bitmap、Recovery Owner CAS 等核心
并发结构全部建立在「`std::atomic` 在共享内存映射上跨进程正确」这一前提之上。
本测试在目标平台基线（Linux x86-64、C++20、GCC ≥ 12 / Clang ≥ 15）上验证：

| # | 测试用例 | 验证点 |
|---|---|---|
| V-12-1 | `BuiltinWidthsAreLockFree` | `std::atomic<uint64_t/uint32_t/uint16_t/bool>` 编译期（`ATOMIC_*_LOCK_FREE == 2`）与运行时（`is_lock_free()`）均为 lock-free；`is_always_lock_free` 静态断言 |
| V-12-2 | `AtomicInt128IsLockFree` | `std::atomic<unsigned __int128>` 在 x86-64 上 lock-free（CMPXCHG16B，需 `-mcx16`），并验证其 fetch_add / CAS 基本行为 |
| V-12-3 | `FetchAddAndCasOnShmMapping` | `shm_open` + `mmap` 共享映射上 placement-new 的 `std::atomic<uint64_t>` 的 fetch_add 累加正确、compare_exchange 成功/失败路径语义正确 |
| V-12-4 | `CrossProcessSequentiallyConsistent` | `fork` 后父子进程映射同一 shm 对象（独立映射、基址可不同），10 万轮严格交替的 seq_cst RMW 操作：校验双方均观测不到乱序与丢失更新，最终计数守恒 |

### 跨进程 Litmus 协议（V-12-4）

```text
父进程                        子进程
  │  turn=0（父可行动）          │
  ├─ counter.fetch_add(i)       │
  ├─ slot.store(counter)        │
  ├─ turn.store(1) ────────────▶│ 等待 turn==1
  │                             ├─ 校验 slot == counter == 期望值
  │                             ├─ counter.fetch_add(i)
  │                             ├─ slot.store(counter)
  │  等待 turn==0/2 ◀───────────├─ turn.store(0)（末轮置 2）
  ├─ 校验 slot == counter       │
```

所有共享访问均为 `memory_order_seq_cst`。任何 store buffer 乱序、缓存一致
性问题或丢失更新都会破坏 `slot == counter == expected` 不变量而被检出。
父子进程通过固定 shm 名（`/mino_v12_litmus`）+ 环境变量传递 fd 的方式
共享映射，规避 Bazel sandbox 与 macOS shm 命名空间差异。

## 2. 运行方式

### Bazel（推荐，CI 使用）

```bash
# Linux x86-64（.bazelrc 的 config:linux-x86_64 自动注入 -mcx16）
bazel test //tests/litmus:atomic_abi_test --test_output=all

# 多配置（sanitizer 互斥，需分别运行）
bazel test --config=debug   //tests/litmus:all
bazel test --config=release //tests/litmus:all
bazel test --config=asan    //tests/litmus:all
bazel test --config=ubsan   //tests/litmus:all
bazel test --config=tsan    //tests/litmus:all
```

> TSAN 说明：跨进程用例使用 seq_cst 交替协议，TSAN 无法跨进程建模
> happens-before，可能对 shm 上的自旋等待给出误报；该用例的结论以
> x86-64 TSO + seq_cst 硬件语义为准，TSAN 主要覆盖进程内用例。

### 直接编译（无 Bazel 环境）

```bash
# Linux
g++ -std=c++20 -mcx16 -Wall -Wextra -g \
    tests/litmus/atomic_abi_test.cc -lgtest -lgtest_main -lrt -latomic -pthread \
    -o /tmp/atomic_abi_test && /tmp/atomic_abi_test

# macOS（冒烟用，128-bit 用例自动 SKIP；Apple Silicon 无 -mcx16）
clang++ -std=c++20 -Wall -Wextra -g \
    tests/litmus/atomic_abi_test.cc -lgtest -lgtest_main \
    -o /tmp/atomic_abi_test && /tmp/atomic_abi_test
```

## 3. 预期结果

Linux x86-64 + GCC ≥ 12 / Clang ≥ 15（带 `-mcx16`）下 **4 个用例全部 PASS**：

```text
[       OK ] AtomicAbiLockFreeTest.BuiltinWidthsAreLockFree
[       OK ] AtomicAbiLockFreeTest.AtomicInt128IsLockFree
[       OK ] AtomicAbiSharedMemoryTest.FetchAddAndCasOnShmMapping
[       OK ] AtomicAbiSharedMemoryTest.CrossProcessSequentiallyConsistent
```

### SKIP 策略（特性不可用时 SKIP，不 FAIL）

| 场景 | 行为 |
|---|---|
| 非 x86-64 平台（如 Apple Silicon）运行 128-bit 用例 | SKIP：未启用 16 字节 CAS（需 `-mcx16`） |
| 非 GCC/Clang 工具链（无 `__int128`） | SKIP：平台不支持 `__int128` |
| 非 POSIX 平台（无 `shm_open`/`fork`） | SKIP：共享内存用例整体跳过 |
| `shm_open` 失败（容器 `/dev/shm` 受限等） | SKIP 并输出 `strerror(errno)` 原因 |
| `fork` 失败 | SKIP 并输出原因 |

## 4. V-12 验证状态

| 验证项 | 状态 | 证据 |
|---|---|---|
| V-12 Linux x86-64 原子 ABI 基线 | **测试已就绪，待目标平台执行** | 本目录 Litmus Test；CI（`.github/workflows/ci.yml`）在 ubuntu-22.04 上按五配置执行，结果作为 Lock-free 验证报告归档 |

判定准则：

- `V-12-1/2/3` 任一失败 → ADR-0001 平台基线不成立，阻塞 D1；
- `V-12-4` 失败 → 跨进程原子语义异常，按风险管理表第 2 条启动编译器/内核
  组合锁定流程；
- 128-bit 用例在目标平台 SKIP → 禁止使用 128-bit 原子于共享结构
  （IndexSlot 等设计需回退 64-bit 方案），并在 ADR-0001 评审记录中备注。
