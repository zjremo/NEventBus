package eventbus

import (
	"context"
	"time"

	"github.com/bwmarrin/snowflake"
)

type EventBus struct {
	registry      *Registry       // topic 与 subscription 注册
	snowflakeNode *snowflake.Node // 雪花算法负责生成唯一的SubscriptionID
	executor      Executor
}

func NewEventBus(poolSize int) *EventBus {
	node, err := snowflake.NewNode(defaultSnowflakeNode)
	if err != nil {
		panic("snowflake newNode failed")
	}

    if poolSize <= 0 {
        poolSize = defaultGoPoolSize
    }

	executor, err := NewExecutor(poolSize)
	if err != nil {
		panic("Executor created failed")
	}

	return &EventBus{
		registry:      NewRegistry(),
		snowflakeNode: node,
		executor:      executor,
	}
}

func (b *EventBus) CreateTopic(topic Topic) {
	b.registry.createTopic(topic)
}

func (b *EventBus) RemoveTopic(topic Topic) {
	b.registry.removeTopic(topic)
}

func (b *EventBus) ListAllTopics() []Topic {
	return b.registry.ListAllTopics()
}

func (b *EventBus) RemoveSub(subID SubscriptionID, waitTime time.Duration) error {
	return b.registry.RemoveSubscription(subID, waitTime)
}

func (b *EventBus) Subscribe(
	topic Topic,
	handler Handler,
	isOnce bool,
	subConcurMode SubConcurrencyMode,
	waitTime time.Duration,
) (sub *Subscription, err error) {
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
        sub.token = make(chan struct{})
	}
	return sub, nil
}

func (b *EventBus) Publish(
	ctx context.Context,
	event *Event,
) (map[SubscriptionID]*Future, error) {

	if event == nil {
		return nil, ErrNilEvent
	}

	subs := b.registry.Lookup(event.Topic)
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
