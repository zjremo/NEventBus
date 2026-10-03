package eventbus_test

import (
	"context"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkPublishSerial_1Sub(b *testing.B) {
	bus := setupBusWithSubs(b, "bench-s1", eventbus.SubSerial, 1)
	defer bus.Close()

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		event, _ := bus.CreateEvent("E", "bench-s1", "bench")
		futures, err := bus.PublishEvent(ctx, event)
		if err != nil {
			b.Fatal(err)
		}
		waitAll(futures)
	}
}
