# EventBus 测试说明

单元测试与基准测试位于同一 `eventbus_test` 包，按功能拆文件，不进入 `eventbus` 实现包，避免测试细节污染生产 API。

## 环境

本次结果采集环境：

| 项 | 值 |
|----|----|
| OS | Linux 7.2.7-zen1-1-zen |
| Arch | linux/amd64 |
| CPU | Intel(R) Core(TM) Ultra 9 285H（16 核） |
| Go | go1.26.4 |
| 命令 | `-count=1`；基准另加 `-benchmem` |

## 单元测试

### 覆盖内容

| 文件 | 用例 | 验证点 |
|------|------|--------|
| `helper_test.go` | `newTestBus` / `mustCreateTopic` / `noopHandler` | 共用构造，`t.Cleanup(Close)` |
| `topic_test.go` | `TestCreateListRemoveTopic` | 创建、列举、删除后列表收敛 |
| | `TestRemoveTopicLazyCleanup` | RemoveTopic 后 Publish 不再投递 |
| | `TestRecreateTopicDoesNotReviveOldSubs` | 同名 Topic 重建不复活旧订阅 |
| `subscribe_test.go` | `TestSubscribeValidation` | 空 Topic / nil Handler / 未建 Topic |
| | `TestFirstSubscribeOnNewTopic` | CreateTopic 后首次订阅成功且有 ID |
| | `TestRemoveSubscription` | 退订后不再触发；二次退订 `ErrSubIDNotFound` |
| `publish_test.go` | `TestPublishParallel` | 并行投递、Future 回传、Metadata 已生成 |
| | `TestPublishMultipleSubscribers` | 5 路扇出，Future 数与调用次数一致 |
| | `TestPublishNilEventAndNoSubscribers` | nil event / 无订阅者 |
| | `TestHandlerErrorPropagates` | Handler error 经 Future 可见 |
| `serial_test.go` | `TestPublishSerialNoOverlap` | 8 路并发 Publish，handler 最大并发度为 1 |
| | `TestPublishSerialQueueFull` | 队列满立即 `ErrSubQueueFull`，不阻塞 Publish |
| `once_test.go` | `TestSubscribeOnce` | Parallel Once 只执行一次 |
| | `TestSubscribeOnceSerial` | Serial Once 只执行一次 |
| `close_test.go` | `TestOperationsAfterClose` | 关闭后所有写/读 API 拒绝 |
| | `TestCloseWithParallelAndSerialSubs` | 混合订阅 Close 不 panic |
| | `TestCloseDrainsInFlight` | 在途 Handler 能完成，Close 不永久卡住 |
| `concurrency_test.go` | `TestConcurrentSubscribePublishUnsubscribe` | 16 订阅 × 32 并发 Publish 后批量退订 |
| | `TestPublishWhenPoolSaturated` | 池大小 1 + Nonblocking，部分 Future 带提交错误 |
| `options_test.go` | `TestNewEventBusWithOptions` | 自定义 PoolSize / SnowflakeNode，TraceID 非空 |
| | `TestCreateTopicIdempotentThenSubscribe` | 重复 CreateTopic 不破坏后续订阅 |
| `panic_test.go` | `TestHandlerPanicParallel` | panic → `ErrHandlerPanic`，池仍可用 |
| | `TestHandlerPanicSerial` | Serial 路径同样隔离 panic |

### 结果

```text
go test ./tests/unit/ -v -count=1
```

| 用例 | 结果 | 耗时 |
|------|------|------|
| TestOperationsAfterClose | PASS | ~0.00s |
| TestCloseWithParallelAndSerialSubs | PASS | ~0.00s |
| TestCloseDrainsInFlight | PASS | 0.05s |
| TestConcurrentSubscribePublishUnsubscribe | PASS | ~0.00s |
| TestPublishWhenPoolSaturated | PASS | ~0.00s |
| TestSubscribeOnce | PASS | ~0.00s |
| TestSubscribeOnceSerial | PASS | ~0.00s |
| TestNewEventBusWithOptions | PASS | ~0.00s |
| TestCreateTopicIdempotentThenSubscribe | PASS | ~0.00s |
| TestHandlerPanicParallel | PASS | ~0.00s |
| TestHandlerPanicSerial | PASS | ~0.00s |
| TestPublishParallel | PASS | ~0.00s |
| TestPublishMultipleSubscribers | PASS | ~0.00s |
| TestPublishNilEventAndNoSubscribers | PASS | ~0.00s |
| TestHandlerErrorPropagates | PASS | ~0.00s |
| TestPublishSerialNoOverlap | PASS | 0.16s |
| TestPublishSerialQueueFull | PASS | ~0.00s |
| TestSubscribeValidation | PASS | ~0.00s |
| TestFirstSubscribeOnNewTopic | PASS | ~0.00s |
| TestRemoveSubscription | PASS | ~0.00s |
| TestCreateListRemoveTopic | PASS | ~0.00s |
| TestRemoveTopicLazyCleanup | PASS | ~0.00s |
| TestRecreateTopicDoesNotReviveOldSubs | PASS | ~0.00s |

**合计：23 PASS，包耗时 0.218s。**

竞态检测：

```text
go test ./tests/unit/ -race -count=1
ok  	eventBus/tests/unit	1.225s
```

未报告 data race。

### 测试角度对照

| 角度 | 关注点 | 对应用例 |
|------|--------|----------|
| API 契约 | 空 Topic、nil Handler/Event、关闭后拒绝 | `TestSubscribeValidation`, `TestOperationsAfterClose` |
| Topic 生命周期 | 创建 / 列举 / 删除 / 重建 | `TestCreateListRemoveTopic`, `TestRecreateTopicDoesNotReviveOldSubs` |
| 惰删 | RemoveTopic 后不再投递；退订清残留 | `TestRemoveTopicLazyCleanup`, `TestRemoveSubscription` |
| 并行投递 | fan-out、结果回传 | `TestPublishParallel`, `TestPublishMultipleSubscribers` |
| 串行投递 | handler 不重叠；队列满不阻塞 | `TestPublishSerialNoOverlap`, `TestPublishSerialQueueFull` |
| Once | Parallel / Serial 只调度一次 | `TestSubscribeOnce*` |
| 错误与 panic | Handler error / panic 隔离 | `TestHandlerErrorPropagates`, `TestHandlerPanic*` |
| 关闭 | 混合订阅、在途 drain | `TestClose*` |
| 池打满 | Nonblocking 下 Future 带错 | `TestPublishWhenPoolSaturated` |
| 配置 | Options、重复 CreateTopic | `TestNewEventBusWithOptions`, `TestCreateTopicIdempotentThenSubscribe` |

## 基准测试

### 覆盖内容

| 文件 | 基准 | 测什么 |
|------|------|--------|
| `create_event_bench_test.go` | `BenchmarkCreateEvent` | 雪花 TraceID/RequestID + Metadata 分配 |
| `subscribe_bench_test.go` | `BenchmarkSubscribeUnsubscribe` | 单 Topic 上 Subscribe + RemoveSub 写路径 |
| `publish_parallel_bench_test.go` | `*_1Sub` / `*_8Subs` | Parallel 投递并 Wait 全部 Future |
| `publish_serial_bench_test.go` | `BenchmarkPublishSerial_1Sub` | Serial 单订阅投递并 Wait |
| `fanout_bench_test.go` | `subs=1/4/16` | 扇出吞吐，并校验 handler 调用次数 |
| `lookup_bench_test.go` | `BenchmarkLookupPath_ManySubs` | 64 订阅下 Lookup + Publish + Wait |
| `concurrent_bench_test.go` | `BenchmarkConcurrentPublish` | 多 goroutine 并发 Publish（4 订阅） |
| | `BenchmarkMixedSubscribePublish` | 8 订阅下并发 Publish |

每轮基准都包含 `CreateEvent` + `PublishEvent` + `Wait`（`CreateEvent` 单独一项除外），数字是**端到端投递延迟**，不是纯 Lookup。

### 结果

```text
go test ./tests/benchmark/ -bench=. -benchmem -count=1
```

| Benchmark | ns/op | B/op | allocs/op | 约合吞吐 |
|-----------|------:|-----:|----------:|----------|
| CreateEvent | 489 | 128 | 4 | ~2.05M 事件/s |
| SubscribeUnsubscribe | 670 | 712 | 11 | ~1.49M 次订阅抖动/s |
| PublishParallel_1Sub | 953 | 440 | 9 | ~1.05M 次投递/s |
| PublishSerial_1Sub | 1315 | 672 | 11 | ~0.76M 次投递/s |
| PublishParallel_8Subs | 4328 | 833 | 23 | ~0.23M 次 Publish/s |
| FanOut/subs=1 | 928 | 440 | 9 | 与 Parallel_1Sub 同量级 |
| FanOut/subs=4 | 2443 | 608 | 15 | ~0.41M 次 Publish/s |
| FanOut/subs=16 | 9924 | 2014 | 41 | ~0.10M 次 Publish/s |
| LookupPath_ManySubs（64） | 45591 | 7370 | 137 | ~21.9K 次 Publish/s |
| ConcurrentPublish（4 订阅，16P） | 3211 | 610 | 15 | ~0.31M ops/s |
| MixedSubscribePublish（8 订阅，16P） | 5754 | 837 | 23 | ~0.17M ops/s |

包耗时 14.280s，全部 PASS。

## 性能分析

### 读路径是快的，成本在“每次 Publish 的配套对象”

`Lookup` 本身是 `hasTopic`（读锁）+ `atomic.Load` + 切片拷贝，不走 CAS。单订阅 Parallel 端到端约 **0.95µs**，其中 `CreateEvent` 就要 **0.49µs / 4 alloc**（两个雪花 ID + `Metadata` + `Event`）。也就是说，空 Handler 场景下大约一半时间花在事件元数据上，而不是 Registry。

单订阅路径大约 9 次分配，主要来自：Event/Metadata、Lookup 切片、`map[SubscriptionID]*Future`、从池取出后仍要配套的 Result。Future 已用 `sync.Pool`，但 Publish 仍按订阅数建 map，扇出一大，分配就上去。

### 写路径已经按 Topic 收敛

`SubscribeUnsubscribe` 约 **670ns / 11 alloc**。当前 COW 只克隆**当前 Topic 的 subSet**，不再整表拷贝，所以单 Topic 上的订阅抖动是轻的。CAS 冲突只发生在同一 Topic 的并发写；跨 Topic 互不影响。这是相对旧“整表 COW”的主要改进。

### Serial 比 Parallel 慢一截，但是稳定的单消费者

同样 1 订阅、空 Handler：Serial **1315ns** vs Parallel **953ns**（约 +38%）。多出来的是：入队、可能拉起消费者、`consume` 出队。换到的是 **handler 最大并发度严格为 1**（`TestPublishSerialNoOverlap`），以及队列满时 **非阻塞失败**（`ErrSubQueueFull`）。适合必须互斥的写模型，不适合当高扇出快路径。

### 扇出近似线性，64 订阅后分配成为主因

| 订阅数 | ns/op | allocs/op | 每订阅约 |
|--------|------:|----------:|----------|
| 1 | 928 | 9 | 928ns |
| 4 | 2443 | 15 | 611ns |
| 8 | 4328 | 23 | 541ns |
| 16 | 9924 | 41 | 620ns |
| 64 | 45591 | 137 | 712ns |

吞吐随订阅数近似线性下降，分配也近似线性（约 `3~6 + 2*N`）。64 路时单次 Publish 约 **7KB / 137 alloc**，其中 `Lookup` 拷贝 64 个指针、再为每条订阅建 Future。这是当前中间件在“中等扇出”下的主要天花板：不是锁，是**每事件每订阅的对象构造**。

并发 Publish（4 订阅、16 goroutine）单次 **3.2µs**，比单线程 4 路扇出（2.4µs）略高，说明池调度和 Future Wait 有争用，但没有出现写锁打满读路径的迹象——符合“读走 atomic、写按 Topic CAS”的设计。

### 池与背压

ants 池 **Nonblocking**：打满时 Parallel 提交失败立刻进 Future，Publish 本身不排队（`TestPublishWhenPoolSaturated`）。这对中间件延迟是好事：调用方用 Wait 感知过载，而不是把延迟藏进无界队列。代价是业务必须处理提交错误；`SubmitFailurePolicy`（Continue / Abort）尚未接到 `triggerTask`。

Serial 队列容量 256，满了同样立刻失败。高突发 + 慢 Handler 时，调用方会先看到 `ErrSubQueueFull`，而不是内存被队列撑爆。

### 对使用方式的含义

- **低扇出、空或短 Handler**：百万级 Publish/s 量级（本机约 1M/s @ 1 订阅 Parallel）。
- **需要互斥**：用 Serial，接受约 30%~40% 的调度开销。
- **高扇出（几十个订阅）**：先优化 Handler 本身；总线侧下一刀应减少 Lookup 拷贝和 Future/map 分配。
- **创建事件很勤**：TraceID/RequestID 已占 CreateEvent 成本的大部分，后续日志/持久化应复用这两枚 ID，而不是再生成一套。

### 后续计划补充的用例

1. Serial 消费者空闲超过 5s 后再 Publish，确认能重新拉起。
2. 极端 CAS 冲突下 `AddSubscription` / `RemoveSubscription` 超时返回。
3. RemoveTopic 后同一 Topic 上多条订阅依次退订（第一条惰删 bucket，后续只清 `byID`）。
4. Handler 内响应 `ctx.Done()`（总线只透传 ctx，行为取决于业务）。

## 命令

```bash
# 单元测试
go test ./tests/unit/ -v -count=1

# 竞态检测
go test ./tests/unit/ -race -count=1

# 基准测试
go test ./tests/benchmark/ -bench=. -benchmem -count=1
```
