package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkLookupPath_ManySubs(b *testing.B) {
	bus := setupBusWithSubs(b, "bench-lookup", eventbus.SubParallel, 64)
	defer bus.Close()

	ctx := context.Background()
	event, _ := bus.CreateEvent("E", "bench-lookup", "warmup")
	futures, _ := bus.PublishEvent(ctx, event)
	waitAll(futures)

	b.ReportAllocs()

	for b.Loop() {
		event, _ := bus.CreateEvent("E", "bench-lookup", "bench")
		futures, err := bus.PublishEvent(ctx, event)
		if err != nil {
			b.Fatal(err)
		}
		waitAll(futures)
	}
}
