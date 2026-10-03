package eventbus_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"eventBus/eventbus"
)

func TestSubscribeOnce(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "once")

	var called atomic.Int32
	sub, err := bus.Subscribe("once", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, true, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		event, _ := bus.CreateEvent("E", "once", "ut")
		futures, err := bus.PublishEvent(context.Background(), event)
		if err != nil {
			t.Fatal(err)
		}
		res := futures[sub.ID()].Wait()
		if i == 0 {
			if res.Err != nil {
				t.Fatalf("first call should succeed: %v", res.Err)
			}
			continue
		}
		if !errors.Is(res.Err, eventbus.ErrSubscriptionAlreadyExecuted) {
			t.Fatalf("call %d: want ErrSubscriptionAlreadyExecuted, got %v", i, res.Err)
		}
	}
	if called.Load() != 1 {
		t.Fatalf("once handler should run once, got %d", called.Load())
	}
}

func TestSubscribeOnceSerial(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "once-serial")

	var called atomic.Int32
	sub, err := bus.Subscribe("once-serial", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, true, eventbus.SubSerial, 0)
	if err != nil {
		t.Fatal(err)
	}

	event1, _ := bus.CreateEvent("E", "once-serial", "1")
	event2, _ := bus.CreateEvent("E", "once-serial", "2")
	f1, _ := bus.PublishEvent(context.Background(), event1)
	f2, _ := bus.PublishEvent(context.Background(), event2)

	if res := f1[sub.ID()].Wait(); res.Err != nil {
		t.Fatal(res.Err)
	}
	if res := f2[sub.ID()].Wait(); !errors.Is(res.Err, eventbus.ErrSubscriptionAlreadyExecuted) {
		t.Fatalf("want ErrSubscriptionAlreadyExecuted, got %v", res.Err)
	}
	if called.Load() != 1 {
		t.Fatalf("called=%d", called.Load())
	}
}
