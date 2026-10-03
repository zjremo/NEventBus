package eventbus

import (
	"context"
	"sync"
	"sync/atomic"
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

var futurePool = sync.Pool{
	New: func() any {
		return &Future{}
	},
}

// Future 包装 result。Wait 可调用多次；第一次 Wait 返回后对象可能被回收进池，下次wait可能等待的是其他的任务结果
type Future struct {
	completed atomic.Bool
	pooled    atomic.Bool
	wg        sync.WaitGroup
	Result    *Result

	ctx   context.Context
	event *Event
	sub   *Subscription
}

func NewFuture(
	ctx context.Context,
	event *Event,
	sub *Subscription,
) *Future {
	f := futurePool.Get().(*Future)
	f.completed.Store(false)
	f.pooled.Store(false)
	f.Result = nil
	f.ctx = ctx
	f.event = event
	f.sub = sub
	f.wg.Add(1)
	return f
}

func (f *Future) complete(result *Result) {
	if !f.completed.CompareAndSwap(false, true) {
		return
	}
	if result == nil {
		result = NewResultOK(nil)
	}
	f.Result = result
	f.wg.Done()
}

func (f *Future) Ready() bool {
	return f.completed.Load()
}

func (f *Future) Wait() *Result {
	f.wg.Wait()
	res := f.Result
	f.recycle()
	return res
}

func (f *Future) recycle() {
	if !f.pooled.CompareAndSwap(false, true) {
		return
	}
	f.ctx = nil
	f.event = nil
	f.sub = nil
	f.Result = nil
	futurePool.Put(f)
}
