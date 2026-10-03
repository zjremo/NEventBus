package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkConcurrentPublish(b *testing.B) {
	bus := setupBusWithSubs(b, "bench-conc", eventbus.SubParallel, 4)
	defer bus.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event, _ := bus.CreateEvent("E", "bench-conc", "bench")
			futures, err := bus.PublishEvent(ctx, event)
			if err != nil {
				b.Error(err)
				continue
			}
			waitAll(futures)
		}
	})
}

func BenchmarkMixedSubscribePublish(b *testing.B) {
	bus := eventbus.NewEventBus(256)
	defer bus.Close()
	_ = bus.CreateTopic("mixed")

	handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		return eventbus.NewResultOK(nil)
	}
	ctx := context.Background()

	const nSubs = 8
	for range nSubs {
		if _, err := bus.Subscribe("mixed", handler, false, eventbus.SubParallel, 0); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event, _ := bus.CreateEvent("E", "mixed", "bench")
			futures, err := bus.PublishEvent(ctx, event)
			if err != nil {
				b.Error(err)
				continue
			}
			waitAll(futures)
		}
	})
}
