package eventbus_test

import (
	"context"
	"sync/atomic"
	"testing"

	"eventBus/eventbus"
)

func TestCreateListRemoveTopic(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "orders")
	mustCreateTopic(t, bus, "payments")

	topics, err := bus.ListAllTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d: %v", len(topics), topics)
	}

	if err := bus.RemoveTopic("orders"); err != nil {
		t.Fatal(err)
	}
	topics, err = bus.ListAllTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0] != "payments" {
		t.Fatalf("unexpected topics after remove: %v", topics)
	}
}

func TestRemoveTopicLazyCleanup(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "lazy")

	var called atomic.Int32
	_, err := bus.Subscribe("lazy", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := bus.RemoveTopic("lazy"); err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "lazy", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if len(futures) != 0 {
		t.Fatalf("removed topic should yield no subscribers, got %d", len(futures))
	}
	if called.Load() != 0 {
		t.Fatal("handler should not run for removed topic")
	}
}

func TestRecreateTopicDoesNotReviveOldSubs(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "revive")

	var called atomic.Int32
	_, err := bus.Subscribe("revive", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := bus.RemoveTopic("revive"); err != nil {
		t.Fatal(err)
	}
	mustCreateTopic(t, bus, "revive")

	event, _ := bus.CreateEvent("E", "revive", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if len(futures) != 0 {
		t.Fatalf("recreated topic should start with no subscribers, got %d", len(futures))
	}
	if called.Load() != 0 {
		t.Fatal("old subscription must not run after topic recreate")
	}

	sub, err := bus.Subscribe("revive", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		called.Add(1)
		return eventbus.NewResultOK(nil)
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}
	event2, _ := bus.CreateEvent("E", "revive", "ut")
	futures2, err := bus.PublishEvent(context.Background(), event2)
	if err != nil {
		t.Fatal(err)
	}
	if res := futures2[sub.ID()].Wait(); res.Err != nil {
		t.Fatal(res.Err)
	}
	if called.Load() != 1 {
		t.Fatalf("new subscription should run once, got %d", called.Load())
	}
}
