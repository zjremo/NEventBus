package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkPublishParallel_1Sub(b *testing.B) {
	bus := setupBusWithSubs(b, "bench-p1", eventbus.SubParallel, 1)
	defer bus.Close()

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		event, _ := bus.CreateEvent("E", "bench-p1", "bench")
		futures, err := bus.PublishEvent(ctx, event)
		if err != nil {
			b.Fatal(err)
		}
		waitAll(futures)
	}
}

func BenchmarkPublishParallel_8Subs(b *testing.B) {
	bus := setupBusWithSubs(b, "bench-p8", eventbus.SubParallel, 8)
	defer bus.Close()

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		event, _ := bus.CreateEvent("E", "bench-p8", "bench")
		futures, err := bus.PublishEvent(ctx, event)
		if err != nil {
			b.Fatal(err)
		}
		waitAll(futures)
	}
}
