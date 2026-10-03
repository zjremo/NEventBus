package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	bus := eventbus.NewEventBus(128)
	defer bus.Close()
	_ = bus.CreateTopic("bench-sub")

	handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		return eventbus.NewResultOK(nil)
	}

	b.ReportAllocs()

	for b.Loop() {
		sub, err := bus.Subscribe("bench-sub", handler, false, eventbus.SubParallel, 0)
		if err != nil {
			b.Fatal(err)
		}
		if err := bus.RemoveSub(sub.ID(), 0); err != nil {
			b.Fatal(err)
		}
	}
}
