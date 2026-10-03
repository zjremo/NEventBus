package eventbus

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
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

	topic Topic

	// subscription 多publish时串行控制
	queue       chan *Future // 存储串行任务
	consumerRun atomic.Bool  // 此时是否存在消费者协程
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

// ID 返回订阅唯一标识，供 RemoveSub 等外部操作使用
func (s *Subscription) ID() SubscriptionID {
	return s.id
}

// invokeHandler 执行业务回调；panic 转为 Future 错误，避免打挂池 worker。
func invokeHandler(sub *Subscription, ctx context.Context, event *Event) (result *Result) {
	defer func() {
		if rec := recover(); rec != nil {
			result = NewResultErr(errors.Wrap(ErrHandlerPanic, fmt.Sprint(rec)))
		}
	}()

	result = sub.handler(ctx, event)
	if result == nil {
		result = NewResultOK(nil)
	}
	return result
}

// consume subscription 单消费者：存活至空闲超时；仅在超时退出时清除 consumerRun，
// 处理中 / 短等待期间保持 true，使并发 Publish 只需入队、不必重复拉起消费者。
func (s *Subscription) consume() {
	for {
		select {
		case future, ok := <-s.queue:
			if !ok {
				return
			}
			future.complete(invokeHandler(s, future.ctx, future.event))

		default:
			timer := time.NewTimer(defaultSubConsumerAliveTimeout)

			select {
			case future, ok := <-s.queue:
				timer.Stop()
				if !ok {
					return
				}
				future.complete(invokeHandler(s, future.ctx, future.event))

			case <-timer.C:
				s.consumerRun.Store(false)
				if len(s.queue) > 0 && s.consumerRun.CompareAndSwap(false, true) {
					continue
				}
				return
			}
		}
	}
}

func (s *Subscription) release() {
	// 并行模式下不创建 queue，避免 close(nil) panic
	if s.queue != nil {
		close(s.queue)
	}
}
