package eventbus_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventBus/eventbus"
)

// TestPublishSerialNoOverlap 验证 SubSerial：同一订阅下多次并发 Publish
// 的 handler 不得重叠执行；不要求事件处理顺序。
func TestPublishSerialNoOverlap(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "serial")

	var (
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
		completed   atomic.Int32
	)

	sub, err := bus.Subscribe("serial", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		cur := inFlight.Add(1)
		// cas无锁计算最大并发度，串行要求只能是1
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}

		// 拉长执行窗口，若实现错误允许并发，inFlight 会大于 1
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		completed.Add(1)
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubSerial, 0)
	if err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	futuresCh := make(chan *eventbus.Future, n)

	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			event, err := bus.CreateEvent("E", "serial", "ut")
			if err != nil {
				errs <- err
				return
			}
			futures, err := bus.PublishEvent(context.Background(), event)
			if err != nil {
				errs <- err
				return
			}
			fut, ok := futures[sub.ID()]
			if !ok {
				errs <- errors.New("missing future for serial subscription")
				return
			}
			futuresCh <- fut
		}()
	}
	wg.Wait()
	close(errs)
	close(futuresCh)

	for err := range errs {
		t.Fatal(err)
	}

	for fut := range futuresCh {
		if res := fut.Wait(); res.Err != nil {
			t.Fatal(res.Err)
		}
	}

	if got := completed.Load(); got != n {
		t.Fatalf("want %d executions, got %d", n, got)
	}
	if max := maxInFlight.Load(); max != 1 {
		t.Fatalf("serial handler overlapped: max in-flight=%d, want 1", max)
	}
}

func TestPublishSerialQueueFull(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "serial-full")

	entered := make(chan struct{})
	block := make(chan struct{})
	sub, err := bus.Subscribe("serial-full", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-block
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubSerial, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "serial-full", "ut")
	first, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	var queued []*eventbus.Future
	sawFull := false
	for range 4096 {
		event, _ := bus.CreateEvent("E", "serial-full", "ut")
		start := time.Now()
		futures, err := bus.PublishEvent(context.Background(), event)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 50*time.Millisecond {
			t.Fatal("Publish blocked on serial queue, want non-blocking full error")
		}

		fut := futures[sub.ID()]
		if fut.Ready() {
			res := fut.Wait()
			if !errors.Is(res.Err, eventbus.ErrSubQueueFull) {
				t.Fatalf("ready future: want ErrSubQueueFull, got %v", res.Err)
			}
			sawFull = true
			break
		}
		queued = append(queued, fut)
	}
	if !sawFull {
		t.Fatal("expected ErrSubQueueFull after filling serial queue")
	}

	close(block)
	if res := first[sub.ID()].Wait(); res.Err != nil {
		t.Fatal(res.Err)
	}
	for _, fut := range queued {
		if res := fut.Wait(); res.Err != nil {
			t.Fatal(res.Err)
		}
	}
}
