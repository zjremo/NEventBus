package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func newTestBus(t *testing.T) eventbus.EventBus {
	t.Helper()
	bus := eventbus.NewEventBus(64)
	t.Cleanup(bus.Close)
	return bus
}

func mustCreateTopic(t *testing.T, bus eventbus.EventBus, topic eventbus.Topic) {
	t.Helper()
	if err := bus.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic(%q): %v", topic, err)
	}
}

func noopHandler(ctx context.Context, event *eventbus.Event) *eventbus.Result {
	return eventbus.NewResultOK(nil)
}
