package eventbus

import (
	"context"
	"sync/atomic"
)

type SubscriptionID string

// Handler 全局adaptor统一适配器
type Handler func(
	ctx context.Context,
	event *Event,
) *Result


type Subscription struct {
	id      SubscriptionID
	handler Handler
	flag    SubscriptionFlag
	mode    SubConcurrencyMode

	called atomic.Bool // once执行时使用
}

// isOnce 是否限制只能执行一次
func (s *Subscription) isOnce() bool {
	return s.flag&FlagOnce != 0
}

func (s *Subscription) isParallel() bool {
    return s.mode&SubParallel != 0
}

func (s *Subscription) getID() SubscriptionID {
	return s.id
}
