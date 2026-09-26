package eventbus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pkg/errors"
)

type EventBus struct {
	registry *Registry
	snowflakeNode *snowflake.Node // 雪花算法负责生成唯一的SubscriptionID
}

func NewEventBus() *EventBus {
	node, err := snowflake.NewNode(defaultSnowflakeNode)
	if err != nil {
		panic("snowflake newNode failed")
	}

	return &EventBus{
		registry:      NewRegistry(),
		snowflakeNode: node,
	}
}

func (b *EventBus) CreateTopic(topic Topic, topicConcurMode TopicConcurrencyMode) {
    b.registry.createTopic(topic, topicConcurMode)
}

func (b *EventBus) RemoveTopic(topic Topic) {
    b.registry.removeTopic(topic)
}

func(b *EventBus) ListAllTopics() map[Topic]*TopicConfig {
    return b.registry.ListAllTopics()
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
	return sub, nil
}

func (b *EventBus) Publish(
	ctx context.Context,
	event *Event,
) (results map[SubscriptionID]*Result, err error) {

	if event == nil {
		return nil, ErrNilEvent
	}

	subs := b.registry.Lookup(event.Topic)
	if len(subs) == 0 {
		return nil, nil
	}

	results = make(map[SubscriptionID]*Result, len(subs))
	errIDs := make([]SubscriptionID, 0, len(subs))

    // executor -> handle 
	for _, sub := range subs {
		if sub.isOnce() {
			if !sub.called.CompareAndSwap(false, true) { // 已经执行过了
				continue
			}
		}

		result := sub.handler(ctx, event)
		if result == nil || result.Err != nil {
			errIDs = append(errIDs, sub.getID())
		}
		results[sub.id] = result
	}

	if len(errIDs) != 0 {
		var bs strings.Builder
		bs.WriteString("以下subscriptionID的subscription执行错误:\n")
		fmt.Fprint(&bs, errIDs)
		err = errors.Wrap(ErrSubExecutionFailed, bs.String())
	}

	return results, err
}
