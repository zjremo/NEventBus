package eventbus

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
)

type EventBus interface {
	// Topic Operations
	CreateTopic(topic Topic) error
	RemoveTopic(topic Topic) error
	ListAllTopics() ([]Topic, error)

	// Subscription Operations
	RemoveSub(subID SubscriptionID, waitTime time.Duration) error
	Subscribe(topic Topic, handler Handler, isOnce bool, subConcurMode SubConcurrencyMode, waitTime time.Duration) (sub *Subscription, err error)

	// Event Operations
	CreateEvent(typeDesc string, topic Topic, source string) (*Event, error)
	PublishEvent(ctx context.Context, event *Event) (map[SubscriptionID]*Future, error)

	// Exit
	Close()
}

var _ EventBus = (*eventBus)(nil)

type eventBus struct {
	registry      *Registry       // topic 与 subscription 注册
	snowflakeNode *snowflake.Node // 雪花算法负责生成唯一的SubscriptionID
	executor      Executor

	closed atomic.Bool
}

func NewEventBus(poolSize int) EventBus {
	return NewEventBusWithOptions(Options{PoolSize: poolSize})
}

func NewEventBusWithOptions(opt Options) EventBus {
	node, err := snowflake.NewNode(opt.snowflakeNode())
	if err != nil {
		panic("snowflake newNode failed: " + err.Error())
	}

	executor, err := NewExecutor(opt.poolSize())
	if err != nil {
		panic("Executor created failed")
	}

	return &eventBus{
		registry:      NewRegistry(),
		snowflakeNode: node,
		executor:      executor,
	}
}

func (b *eventBus) checkClose() bool {
	return b.closed.Load()
}

func (b *eventBus) CreateTopic(topic Topic) error {
	if b.checkClose() {
		return ErrEventBusClosed
	}

	b.registry.createTopic(topic)
	return nil
}

func (b *eventBus) RemoveTopic(topic Topic) error {
	if b.checkClose() {
		return ErrEventBusClosed
	}

	b.registry.removeTopic(topic)
	return nil
}

func (b *eventBus) ListAllTopics() ([]Topic, error) {
	if b.checkClose() {
		return nil, ErrEventBusClosed
	}

	return b.registry.ListAllTopics(), nil
}

func (b *eventBus) RemoveSub(subID SubscriptionID, waitTime time.Duration) error {
	if b.checkClose() {
		return ErrEventBusClosed
	}

	return b.registry.RemoveSubscription(subID, waitTime)
}

func (b *eventBus) Subscribe(
	topic Topic,
	handler Handler,
	isOnce bool,
	subConcurMode SubConcurrencyMode,
	waitTime time.Duration,
) (sub *Subscription, err error) {

	if b.checkClose() {
		return nil, ErrEventBusClosed
	}

	if len(topic) == 0 {
		return nil, ErrEmptyTopic
	}

	if handler == nil {
		return nil, ErrNilHandler
	}

	id := b.snowflakeNode.Generate().Base64()
	sub = &Subscription{
		id:      SubscriptionID(id),
		handler: handler,
		mode:    subConcurMode,
	}

	if isOnce {
		sub.flag |= FlagOnce
	}

	if err = b.registry.AddSubscription(topic, sub, waitTime); err != nil {
		return nil, err
	}

	if !sub.isParallel() { // 此时是串行执行
		sub.queue = make(chan *Future, defaultSubQueueSize)
	}
	return sub, nil
}

func (b *eventBus) PublishEvent(
	ctx context.Context,
	event *Event,
) (map[SubscriptionID]*Future, error) {

	if b.checkClose() {
		return nil, ErrEventBusClosed
	}

	if event == nil {
		return nil, ErrNilEvent
	}

	subs := b.registry.Lookup(event.topic)
	if len(subs) == 0 {
		return nil, nil
	}

	futures, err := b.executor.submitTask(ctx, event, subs)
	if err != nil {
		return futures, err
	}

	b.executor.triggerTask(futures)
	return futures, nil
}

func (b *eventBus) CreateEvent(typeDesc string, topic Topic, source string) (*Event, error) {
	if b.checkClose() {
		return nil, ErrEventBusClosed
	}

	metadata := &Metadata{
		traceID:   b.snowflakeNode.Generate().Base36(),
		requestID: b.snowflakeNode.Generate().Base36(),
		source:    source,
	}

	return &Event{
		typeDesc: typeDesc,
		topic:    topic,
		metadata: metadata,
	}, nil
}

func (b *eventBus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}

	// 先关闭 Serial 队列：唤醒消费者并排空已入队任务
	b.registry.release()
	// 再等待池内 handler / 消费者退出（超时后不再阻塞 Close）
	b.executor.close()
}
