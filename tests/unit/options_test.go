package eventbus_test

import (
	"testing"

	"eventBus/eventbus"
)

func TestNewEventBusWithOptions(t *testing.T) {
	bus := eventbus.NewEventBusWithOptions(eventbus.Options{
		PoolSize:         8,
		SnowflakeNode:    7,
	})
	t.Cleanup(bus.Close)

	mustCreateTopic(t, bus, "opt")
	sub, err := bus.Subscribe("opt", noopHandler, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID() == "" {
		t.Fatal("empty subscription id")
	}

	event, err := bus.CreateEvent("E", "opt", "ut")
	if err != nil {
		t.Fatal(err)
	}
	if event.Metadata().TraceID() == "" {
		t.Fatal("empty trace id")
	}
}

func TestCreateTopicIdempotentThenSubscribe(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "dup")
	mustCreateTopic(t, bus, "dup")

	if _, err := bus.Subscribe("dup", noopHandler, false, eventbus.SubParallel, 0); err != nil {
		t.Fatalf("second CreateTopic must not wipe subscribers capability: %v", err)
	}
}
