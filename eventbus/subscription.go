package eventbus

import (
	"context"
	"sync/atomic"
	"time"
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

    // subscription 多publish时串行控制
    queue chan *Future // 存储串行任务 
    consumerRun atomic.Bool // 此时是否存在消费者协程
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

// consume subscription单消费者handler函数
func (s *Subscription) consume() {
    for {
        select {
        case future, ok := <- s.queue:
            if !ok {
                return
            }

            result := s.handler(
                future.ctx,
                future.event,
            )
            future.complete(result)

        default:
            timer := time.NewTimer(defaultSubConsumerAliveTimeout)

            select {
            case future, ok := <- s.queue:
                if !ok {
                    return
                }

                result := s.handler(
                    future.ctx,
                    future.event,
                )
                future.complete(result)

            case <-timer.C:
                return
            }
        }
    }
}

func (s *Subscription) release() {
    close(s.queue)
}
