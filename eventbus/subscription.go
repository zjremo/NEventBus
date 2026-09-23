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

type ExecMode uint8

const (
    ExecSync ExecMode = iota // 同步执行
    ExecAsyncConcur // 异步, 支持并发
    ExecAsyncSerial // 异步，串行
)

type SubscriptionFlag uint8

const (
    FlagOnce SubscriptionFlag = 1 // 限制只能执行一次
)

type Subscription struct {
    id SubscriptionID
    handler Handler
    flag SubscriptionFlag
    mode ExecMode

    called atomic.Bool // once执行时使用
}

// isOnce 是否限制只能执行一次
func (s *Subscription) isOnce() bool {
    return s.flag & FlagOnce != 0
}

func (s *Subscription) getID() SubscriptionID {
    return s.id
}
