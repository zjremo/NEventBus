package eventbus

import (
	"context"
)

type Result struct {
	Data any
	Err  error
}

func NewResult() *Result {
	return &Result{}
}

func NewResultOK(data any) *Result {
	return &Result{
		Data: data,
	}
}

func NewResultErr(err error) *Result {
	return &Result{
		Err: err,
	}
}

// Future 包装result，同时利用channel来解耦提交和执行过程
type Future struct {
	done   chan struct{} // subscription执行成功
	Result *Result

	ctx   context.Context
	event *Event
	sub   *Subscription
}

func NewFuture(
	ctx context.Context,
	event *Event,
	sub *Subscription,
) *Future {
	return &Future{
		done:  make(chan struct{}),
		ctx:   ctx,
		event: event,
		sub:   sub,
	}
}

func (f *Future) complete(result *Result) {
	f.Result = result
	close(f.done)
}

func (f *Future) Wait() *Result {
    <-f.done
    return f.Result
}
