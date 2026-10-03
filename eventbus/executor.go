package eventbus

import (
	"context"

	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
)

var _ Executor = (*executor)(nil)

type Executor interface {
	submitTask(ctx context.Context, event *Event, subs []*Subscription) (map[SubscriptionID]*Future, error)
	triggerTask(futures map[SubscriptionID]*Future)

	// exit
	close()
}

type executor struct {
	pool *ants.Pool
}

func NewExecutor(size int) (Executor, error) {
	pool, err := ants.NewPool(
		size,
		ants.WithNonblocking(true),
	)

	if err != nil {
		return nil, errors.Wrap(err, "create executor goroutine pool")
	}

	return &executor{
		pool: pool,
	}, nil
}

func (e *executor) submitTask(ctx context.Context, event *Event, subs []*Subscription) (map[SubscriptionID]*Future, error) {
	futures := make(map[SubscriptionID]*Future, len(subs))

	for _, sub := range subs {
		futures[sub.id] = NewFuture(ctx, event, sub)
	}

	return futures, nil
}

func (e *executor) triggerTask(futures map[SubscriptionID]*Future) {
	for _, future := range futures {
		switch future.sub.mode {
		case SubSerial:
			e.triggerSerial(future)
		case SubParallel:
			e.triggerParallel(future)
		default:
			future.complete(NewResultErr(ErrInvalidSubMode))
		}
	}
}

func (e *executor) triggerSerial(future *Future) {
	if future.sub.isOnce() && !future.sub.called.CompareAndSwap(false, true) {
		future.complete(NewResultErr(ErrSubscriptionAlreadyExecuted))
		return
	}

	e.enqueueSub(future)
}

func (e *executor) enqueueSub(future *Future) {
	sub := future.sub

	if sub.consumerRun.CompareAndSwap(false, true) {
		err := e.pool.Submit(func() {
			sub.consume()
		})
		if err != nil {
			sub.consumerRun.Store(false)
			future.complete(NewResultErr(errors.Wrap(err, "submit consumer task to pool")))
			return
		}
	}

	e.tryEnqueue(sub, future)
}

func (e *executor) tryEnqueue(sub *Subscription, future *Future) {
	defer func() {
		if rec := recover(); rec != nil {
			// Close 后 queue 已关闭：send panic，视为投递失败
			future.complete(NewResultErr(ErrEventBusClosed))
		}
	}()

	select {
	case sub.queue <- future:
	default:
		future.complete(NewResultErr(ErrSubQueueFull))
	}
}

func (e *executor) triggerParallel(future *Future) {
	if future.sub.isOnce() && !future.sub.called.CompareAndSwap(false, true) {
		future.complete(NewResultErr(ErrSubscriptionAlreadyExecuted))
		return
	}

	err := e.pool.Submit(func() {
		future.complete(invokeHandler(future.sub, future.ctx, future.event))
	})

	if err != nil {
		future.complete(NewResultErr(errors.Wrap(err, "submit task to executor pool")))
	}
}

func (e *executor) close() {
	_ = e.pool.ReleaseTimeout(defaultCloseDrainTimeout)
}
