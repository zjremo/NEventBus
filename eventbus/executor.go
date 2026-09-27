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

	select {
	case <-future.ctx.Done():
		future.complete(NewResultErr(future.ctx.Err()))
    default:
        future.sub.token <- struct{}{}
        defer func(){
            <-future.sub.token
        }()
        result := future.sub.handler(future.ctx, future.event)
        future.complete(result)
	}
}

func (e *executor) triggerParallel(future *Future) {
	if future.sub.isOnce() && !future.sub.called.CompareAndSwap(false, true) {
		future.complete(NewResultErr(ErrSubscriptionAlreadyExecuted))
		return
	}

	err := e.pool.Submit(func() {
		result := future.sub.handler(
			future.ctx,
			future.event,
		)
		future.complete(result)
	})

	if err != nil {
		future.complete(NewResultErr(errors.Wrap(err, "submit task to executor pool")))
	}
}
