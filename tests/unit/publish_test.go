package eventbus_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"eventBus/eventbus"
)

func TestPublishParallel(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "p")

	var called atomic.Int32
	sub, err := bus.Subscribe("p", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK("ok")
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, err := bus.CreateEvent("E", "p", "ut")
	if err != nil {
		t.Fatal(err)
	}
	if event.Metadata().TraceID() == "" || event.Metadata().RequestID() == "" {
		t.Fatal("metadata IDs should be generated")
	}

	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	fut, ok := futures[sub.ID()]
	if !ok {
		t.Fatal("missing future for subscription")
	}
	res := fut.Wait()
	if res.Err != nil {
		t.Fatalf("handler error: %v", res.Err)
	}
	if res.Data != "ok" {
		t.Fatalf("unexpected data: %v", res.Data)
	}
	if called.Load() != 1 {
		t.Fatalf("handler called %d times", called.Load())
	}
}

func TestPublishMultipleSubscribers(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "multi")

	const n = 5
	var called atomic.Int32
	subs := make([]*eventbus.Subscription, 0, n)
	for range n {
		sub, err := bus.Subscribe("multi", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
			called.Add(1)
			return eventbus.NewResultOK(nil)
		}, false, eventbus.SubParallel, 0)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	event, _ := bus.CreateEvent("E", "multi", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if len(futures) != n {
		t.Fatalf("want %d futures, got %d", n, len(futures))
	}
	for _, sub := range subs {
		if res := futures[sub.ID()].Wait(); res.Err != nil {
			t.Fatal(res.Err)
		}
	}
	if called.Load() != n {
		t.Fatalf("want %d calls, got %d", n, called.Load())
	}
}

func TestPublishNilEventAndNoSubscribers(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "empty")

	if _, err := bus.PublishEvent(context.Background(), nil); !errors.Is(err, eventbus.ErrNilEvent) {
		t.Fatalf("want ErrNilEvent, got %v", err)
	}

	event, _ := bus.CreateEvent("E", "empty", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if futures != nil {
		t.Fatalf("expected nil futures map, got %#v", futures)
	}
}

func TestHandlerErrorPropagates(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "err")

	want := errors.New("boom")
	sub, err := bus.Subscribe("err", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		return eventbus.NewResultErr(want)
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "err", "ut")
	futures, _ := bus.PublishEvent(context.Background(), event)
	res := futures[sub.ID()].Wait()
	if !errors.Is(res.Err, want) {
		t.Fatalf("want %v, got %v", want, res.Err)
	}
}
