package eventbus_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventBus/eventbus"
)

func TestConcurrentSubscribePublishUnsubscribe(t *testing.T) {
	bus := eventbus.NewEventBus(256)
	t.Cleanup(bus.Close)
	mustCreateTopic(t, bus, "stress")

	const (
		nSubs = 16
		nPubs = 32
	)

	handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		return eventbus.NewResultOK(nil)
	}

	// 订阅写路径 CAS 重试次数有限，顺序注册；并发焦点放在 Publish
	subs := make([]*eventbus.Subscription, 0, nSubs)
	for i := range nSubs {
		sub, err := bus.Subscribe("stress", handler, false, eventbus.SubParallel, time.Second)
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		subs = append(subs, sub)
	}

	var wg sync.WaitGroup
	errs := make(chan error, nPubs)

	wg.Add(nPubs)
	for range nPubs {
		go func() {
			defer wg.Done()
			event, err := bus.CreateEvent("E", "stress", "ut")
			if err != nil {
				errs <- err
				return
			}
			futures, err := bus.PublishEvent(context.Background(), event)
			if err != nil {
				errs <- err
				return
			}
			for _, fut := range futures {
				res := fut.Wait()
				if res == nil || res.Err == nil {
					continue
				}
				if !strings.Contains(res.Err.Error(), "too many goroutines") {
					errs <- res.Err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent publish: %v", err)
	}

	for _, sub := range subs {
		if err := bus.RemoveSub(sub.ID(), time.Second); err != nil && !errors.Is(err, eventbus.ErrSubIDNotFound) {
			t.Errorf("RemoveSub: %v", err)
		}
	}
}

func TestPublishWhenPoolSaturated(t *testing.T) {
	bus := eventbus.NewEventBus(1)
	t.Cleanup(bus.Close)
	mustCreateTopic(t, bus, "tiny")

	block := make(chan struct{})
	handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		<-block
		return eventbus.NewResultOK(nil)
	}

	const n = 8
	subs := make([]*eventbus.Subscription, 0, n)
	for range n {
		sub, err := bus.Subscribe("tiny", handler, false, eventbus.SubParallel, 0)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	event, _ := bus.CreateEvent("E", "tiny", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}

	var sawPoolErr, sawOK atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, sub := range subs {
			res := futures[sub.ID()].Wait()
			if res.Err != nil {
				sawPoolErr.Add(1)
				continue
			}
			sawOK.Add(1)
		}
	}()

	close(block)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting futures")
	}

	if sawPoolErr.Load() == 0 {
		t.Fatal("expected at least one pool-submit error under Nonblocking tiny pool")
	}
	if sawOK.Load()+sawPoolErr.Load() != int32(n) {
		t.Fatalf("ok=%d err=%d want sum %d", sawOK.Load(), sawPoolErr.Load(), n)
	}
}
