# Local EventBus

> 模块路径：`eventBus/eventbus`  
> 定位：进程内本地事件总线（Local EventBus）。当前版本完成 Topic / 订阅 / 投递 / 关闭的完整闭环，后续可扩展日志追踪、配置热启动与 Network EventBus。

## 1. 设计目标

- **Topic 驱动**：先创建 Topic，再按 Topic 订阅与投递。
- **订阅灵活**：支持 Once、Serial（单消费者串行）、Parallel（池内并行）。
- **读写分离**：读路径（`Lookup`）只做 `hasTopic` + `atomic.Load`；写路径按 Topic 做 Copy-on-Write + CAS。
- **删除不阻塞读**：`RemoveTopic` 只摘权威标记，bucket 在后续退订时惰删。
- **异步可等待**：`PublishEvent` 立即返回 `map[SubscriptionID]*Future`，调用方按需 `Wait()`。
- **资源可控**：Handler 与 Serial 消费者都走 `ants` 协程池（Nonblocking），避免无界 goroutine。
- **可配置启动**：`Options` 控制池大小与雪花 Node；事件已携带 `TraceID` / `RequestID`，为后续日志与持久化追踪留口。

## 2. 模块结构

```
eventbus/
├── bus.go           # EventBus 门面：Topic / Subscribe / Publish / Close
├── registry.go      # Topic 权威表 + 每 Topic 订阅桶（COW + CAS + 惰删）
├── executor.go      # 任务提交与触发（Serial 入队 / Parallel 投池）
├── subscription.go  # 订阅实体、Serial 单消费者、Handler panic 隔离
├── event.go         # Event / Topic / Metadata（TraceID / RequestID / Source）
├── model.go         # Result / Future（sync.Pool 复用）
├── options.go       # 启动配置：PoolSize / SnowflakeNode
├── const.go         # 超时、队列容量、并发模式常量
└── error.go         # 错误定义
```

### 2.1 核心对象

```
eventBus
  ├── Registry
  │     ├── topicMap   (mutex)     Topic 是否“正式存在”
  │     ├── buckets    (mutex)     Topic → topicEntry
  │     │                              └── atomic.Pointer[subSet]
  │     └── byID       (sync.Map)  SubscriptionID → *Subscription
  ├── snowflake.Node               SubscriptionID / TraceID / RequestID
  └── Executor
        └── ants.Pool              Handler + Serial 消费者
```

## 3. 整体流程

### 3.1 启动

```
NewEventBus(poolSize)
  └─ NewEventBusWithOptions(Options{PoolSize})

NewEventBusWithOptions(opt)
  ├─ snowflake.NewNode(opt.snowflakeNode())   // <=0 则用 defaultSnowflakeNode=22
  ├─ NewExecutor(opt.poolSize())              // <=0 则用 defaultGoPoolSize=100
  │     └─ ants.NewPool(size, Nonblocking)
  └─ NewRegistry()
```

公开构造：

```go
bus := eventbus.NewEventBus(64)
bus := eventbus.NewEventBusWithOptions(eventbus.Options{
    PoolSize:      256,
    SnowflakeNode: 7,
})
```

### 3.2 Topic 生命周期

`topicMap` 是 Topic 是否对外可见的权威标记。`buckets` 持有该 Topic 的订阅集合指针，可短暂残留，后续进行惰性删除。

| 操作 | topicMap | buckets |
|------|----------|---------|
| `CreateTopic` | 写入 | 没有则新建 `topicEntry`，并 `Store` 空 `subSet`（重建 Topic 时覆盖旧集合，旧订阅不会复活） |
| `RemoveTopic` | 删除 | **不删**（惰删，留给后续退订） |
| `ListAllTopics` | 只读拷贝 | 不读 |
| `Lookup` | 不存在则直接空 | 仅当 Topic 仍正式存在时才 Load 订阅集合 |

```
CreateTopic(topic)
  lock topicMapMutex
  if topicMap[topic] 已存在 → return
  buckets[topic] 不存在则新建 topicEntry
  entry.subs.Store(空 subSet)
  topicMap[topic] = {}
  unlock

RemoveTopic(topic)
  lock topicMapMutex
  delete(topicMap, topic)          // buckets 残留
  unlock
```

因此：`RemoveTopic` 之后 `Publish` / `Lookup` 立即看不到订阅者；旧 `*Subscription` 仍可能留在 `byID` 与残留 bucket 中，直到退订时清理。

### 3.3 订阅 / 退订

```
Subscribe(topic, handler, isOnce, mode, waitTime)
  ├─ 关闭 / 空 Topic / nil Handler → 对应错误
  ├─ 雪花 ID → Subscription
  ├─ Registry.AddSubscription
  │     ├─ Topic 不在 topicMap 或没有 bucket → ErrTopicNotFound
  │     └─ 对该 Topic 的 subSet：Load → clone → 写入 → CAS
  │           失败则 Gosched 重试，直到 waitTime（默认 2s）
  └─ Serial 才创建 queue（容量 defaultSubQueueSize=256）

RemoveSub(subID, waitTime)
  └─ Registry.RemoveSubscription
        ├─ byID 没有 → ErrSubIDNotFound
        ├─ topic 已不在 topicMap
        │     ├─ buckets 仍有该 Topic → 删除 buckets[topic]，再删 byID
        │     └─ buckets 已无该 Topic → 只删 byID（后续退订走这条）
        └─ topic 仍存在
              └─ 对该 Topic 的 subSet：Load → clone → delete → CAS
                    失败则重试至 waitTime（默认 2s）
```

写路径只克隆**当前 Topic** 的 `subSet`，不再整表拷贝。CAS 冲突窗口是“同一 Topic 上的并发订阅/退订”。

### 3.4 事件创建与投递

```
CreateEvent(typeDesc, topic, source)
  └─ Metadata{ TraceID, RequestID, Source }   // 两个雪花 Base36

PublishEvent(ctx, event)
  ├─ 关闭 / nil event → 错误
  ├─ Registry.Lookup(event.topic)
  │     ├─ hasTopic == false → nil（已 RemoveTopic 的 Topic 不会投递）
  │     └─ atomic.Load(subSet) → 拷贝 []*Subscription
  ├─ 无订阅者 → (nil, nil)
  ├─ Executor.submitTask → 为每个订阅 NewFuture（sync.Pool）
  └─ Executor.triggerTask
        ├─ SubParallel
        │     ├─ Once：called CAS，失败则 ErrSubscriptionAlreadyExecuted
        │     └─ pool.Submit(invokeHandler) ；池满 → Future 带提交错误
        └─ SubSerial
              ├─ Once：同上
              ├─ 若尚无消费者：CAS consumerRun 后 Submit(consume)
              └─ 非阻塞入队；队列满 → ErrSubQueueFull（不阻塞 Publish）
```

`invokeHandler` 会 recover panic，转为 `ErrHandlerPanic`，避免打挂池 worker。

### 3.5 Serial 消费者

单个订阅同一时刻最多一个消费者 goroutine：

1. 从 `queue` 取 `Future`，调用 Handler 后 `complete`。
2. 队列空时等 `defaultSubConsumerAliveTimeout`（5s）；超时将 `consumerRun` 置 false。
3. 置 false 后若队列又进了任务，CAS 抢回消费者身份并继续，避免任务滞留。
4. `Close` 时 `release()` 关闭 queue，消费者在 `!ok` 时退出。

Parallel 订阅不创建 queue，`release` 对 `queue == nil` 直接跳过。

### 3.6 Future

```
Wait()  → wg.Wait() → 取 Result → recycle 回 sync.Pool
Ready() → completed 原子标记
complete() 只成功一次（CAS），防止二次 complete
```

`Wait` 可在 complete 前多次并发调用（`WaitGroup`）；**第一次 `Wait` 返回后对象可能被回收**，不要对同一 `*Future` 再 `Wait`。

### 3.7 关闭

```
Close()
  ├─ closed CAS false→true（幂等）
  ├─ registry.release()     // 关闭所有 Serial queue，唤醒消费者
  └─ executor.close()       // pool.ReleaseTimeout(5s)
```

关闭后 `CreateTopic` / `Subscribe` / `PublishEvent` 等返回 `ErrEventBusClosed`。在途 Handler 尽量排空后再释放池。

## 4. 公开 API 速览

```go
bus := eventbus.NewEventBusWithOptions(eventbus.Options{
    PoolSize:      64,
    SnowflakeNode: 22,
})
defer bus.Close()

_ = bus.CreateTopic("order.created")

sub, _ := bus.Subscribe("order.created", handler, false, eventbus.SubParallel, 0)

event, _ := bus.CreateEvent("OrderCreated", "order.created", "checkout-svc")
// event.Metadata().TraceID() / RequestID() / Source()

futures, _ := bus.PublishEvent(ctx, event)
_ = futures[sub.ID()].Wait()

_ = bus.RemoveSub(sub.ID(), 0)
_ = bus.RemoveTopic("order.created")
```

## 5. 关键设计取舍

### 5.1 Registry：权威标记 + 每 Topic COW

| 结构 | 作用 |
|------|------|
| `topicMap` | Topic 是否正式存在；`Lookup` / `AddSubscription` 先看它 |
| `buckets` | Topic → `atomic.Pointer[subSet]`；读无锁 Load，写 COW+CAS |
| `byID` | 按 ID 退订的索引；`RemoveTopic` 后仍可能残留，退订时清掉 |

- `RemoveTopic` 不扫订阅、不阻塞 `Lookup` 的 atomic Load。
- 惰删发生在 `RemoveSubscription`：Topic 已死且 bucket 还在 → 删 bucket；后续再退订同一 Topic 下的订阅 → 只删 `byID`。
- `CreateTopic` 重建同名 Topic 时写入**新的空集合**，旧订阅不会被 `Lookup` 看到。

### 5.2 执行模型

| 模式 | 行为 |
|------|------|
| `SubParallel` | 每次 Publish 向 ants 池提交独立任务，同一订阅可并发执行 |
| `SubSerial` | 入 `subscription.queue`，单消费者串行；空闲 5s 后消费者退出 |

`FlagOnce`：`called` 的 CAS 保证 Handler 只成功调度一次。

池为 **Nonblocking**：打满时 Parallel 提交失败、Serial 拉消费者失败，都通过 Future 返回错误，不阻塞 Publish。Serial 队列满同样立即 `ErrSubQueueFull`。

### 5.3 当前仍开放的点

- Serial 消费者超时退出与入队之间仍有短窗口，依赖下一次 Publish 再拉起消费者。
- `Lookup` 每次拷贝订阅切片；高扇出时分配随订阅数线性增长。
- `TraceID` / `RequestID` 已生成并暴露 getter，总线本身尚未写日志、尚未落盘。

## 6. 未来规划

### 6.1 用 TraceID / RequestID 做日志与持久化追踪

`CreateEvent` 已为每条事件生成：

- **TraceID**：一次业务链路的追踪号，可贯穿多次 Publish / 后续跨进程投递。
- **RequestID**：单次事件实例号，区分同一 Trace 下的重复投递、重试与 Replay。
- **Source**：事件来源服务或模块。

规划：

1. **结构化日志**：Subscribe / Publish / Handler 完成 / 错误 / panic 统一带上 `trace_id`、`request_id`、`topic`、`sub_id`，便于按链路检索。
2. **投递生命周期**：记录 `accepted → dispatched → completed|failed`，与 Future 结果对齐。
3. **持久化追踪**：将上述记录写入 WAL / 本地文件 / 外部 store，支持按 RequestID 查询“这条事件投给了哪些订阅、各自结果是什么”。
4. **重试与 Replay**：失败事件按 RequestID 去重，按 TraceID 回放整条链路；Once 订阅与幂等 Handler 可据此二次消费。

### 6.2 二次利用配置启动

当前 `Options` 只覆盖池大小与雪花 Node。规划把“可再次拉起总线”的状态显式配置化：

1. **启动配置**：`PoolSize`、`SnowflakeNode`、Serial 队列容量、消费者空闲超时、Close drain 超时、日志与持久化后端。
2. **拓扑配置**：Topic 列表、订阅描述（Topic、Once、Serial/Parallel、Handler 注册名），进程重启后按配置重建，而不是只靠运行时 API。
3. **配置复用**：同一份配置用于开发 / 测试 / 生产启动；支持从文件或环境加载，校验后 `NewEventBusWithOptions` + 批量 `CreateTopic` / `Subscribe`。
4. **与追踪结合**：重启后新事件继续分配 RequestID，可选继承或关联旧 TraceID，使落盘记录在二次启动后仍能串起来。

### 6.3 其它演进

- 接入 `SubmitFailurePolicy`：池满或单订阅失败时选择继续扇出或中止。
- Network EventBus：本地 Registry 作路由面，TraceID 作为跨节点关联键。
- `Lookup` 零拷贝或分代快照，降低高扇出分配。

## 7. 测试

详见 [`tests/README.md`](tests/README.md)（用例说明、实测结果与性能分析）。

```bash
go test ./tests/unit/ -v -count=1
go test ./tests/unit/ -race -count=1
go test ./tests/benchmark/ -bench=. -benchmem -count=1
```
