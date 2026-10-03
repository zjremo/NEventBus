package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func setupBusWithSubs(b *testing.B, topic eventbus.Topic, mode eventbus.SubConcurrencyMode, nSubs int) eventbus.EventBus {
	b.Helper()
	bus := eventbus.NewEventBus(256)
	if err := bus.CreateTopic(topic); err != nil {
		b.Fatal(err)
	}

	handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		return eventbus.NewResultOK(nil)
	}

	for range nSubs {
		if _, err := bus.Subscribe(topic, handler, false, mode, 0); err != nil {
			b.Fatal(err)
		}
	}
	return bus
}

func waitAll(futures map[eventbus.SubscriptionID]*eventbus.Future) {
	for _, fut := range futures {
		fut.Wait()
	}
}
