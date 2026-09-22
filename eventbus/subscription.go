package eventbus

import (
	"context"
	"sync/atomic"
)

type SubscriptionID uint64

// Handler 全局adaptor统一适配器
type Handler func(
    ctx context.Context,
    event *Event,
) error

/* 
SubscriptionFlag 标识事件处理类型
    1. 立即执行;
    2. 只能执行一次;
    3. async 异步支持并发;
    4. async 异步不支持并发.
*/
type SubscriptionFlag uint8

const (
    FlagNone SubscriptionFlag = 0
    FlagOnce SubscriptionFlag = 1 << iota
    FlagAsyncConcur 
    FlagAsyncSerial
)

type Subscription struct {
    id SubscriptionID
    handler Handler
    flags SubscriptionFlag

    called atomic.Bool
}
