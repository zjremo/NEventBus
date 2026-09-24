# EventBus 改造

Two parts:
1. Local EventBus;
2. NetWork EventBus.

Concurrency:

- 多个goroutine同时publish消息

一个消息对应的subscriptions如何执行呢？ 即publish的并发控制:

1. goroutine之间无并发控制，无顺序要求； ====> 统一放到gopool里面 

2. goroutine之间需要串行完成； ====> topic层面上锁，每次都要获取锁才能执行 
这时可以利用sync.Mutex上锁，因为就锁了一个topic，其实锁的粒度并不大

3. goroutine之间可以并发，但是同一个subId的subscription只能串行执行;
此时对subscription进行上锁，粒度比较小，subscription里面串行执行

4. goroutine之间可以并发，同一个subId的subscription也可以并行完成
纯并行，统一放到gopool里面。

并发度一定得控制，否则极其容易打爆机器

字段拆分:

1. ConcurrencyMode: 并发控制 ===> 同时运用到subscription and topic control

2. topic 的正确性查哪里呢？
TopicMap key: Topic --> value: TopicConfig

remove a topic -> delete topic from TopicMap
how to deal with topics ===> cas   goroutine NewTimer
