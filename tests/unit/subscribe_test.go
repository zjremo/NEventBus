package eventbus_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"eventBus/eventbus"
)

func TestSubscribeValidation(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "t")

	if _, err := bus.Subscribe("", noopHandler, false, eventbus.SubParallel, 0); !errors.Is(err, eventbus.ErrEmptyTopic) {
		t.Fatalf("empty topic: want ErrEmptyTopic, got %v", err)
	}
	if _, err := bus.Subscribe("t", nil, false, eventbus.SubParallel, 0); !errors.Is(err, eventbus.ErrNilHandler) {
		t.Fatalf("nil handler: want ErrNilHandler, got %v", err)
	}
	if _, err := bus.Subscribe("missing", noopHandler, false, eventbus.SubParallel, 0); !errors.Is(err, eventbus.ErrTopicNotFound) {
		t.Fatalf("missing topic: want ErrTopicNotFound, got %v", err)
	}
}

func TestFirstSubscribeOnNewTopic(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "first")

	sub, err := bus.Subscribe("first", noopHandler, false, eventbus.SubParallel, time.Second)
	if err != nil {
		t.Fatalf("first subscribe must succeed, got %v", err)
	}
	if sub.ID() == "" {
		t.Fatal("subscription ID should not be empty")
	}
}

func TestRemoveSubscription(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "rm")

	var called atomic.Int32
	sub, err := bus.Subscribe("rm", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := bus.RemoveSub(sub.ID(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := bus.RemoveSub(sub.ID(), time.Second); !errors.Is(err, eventbus.ErrSubIDNotFound) {
		t.Fatalf("second remove: want ErrSubIDNotFound, got %v", err)
	}

	event, _ := bus.CreateEvent("E", "rm", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if len(futures) != 0 {
		t.Fatalf("expected no futures after remove, got %d", len(futures))
	}
	if called.Load() != 0 {
		t.Fatalf("handler should not be called, got %d", called.Load())
	}
}
