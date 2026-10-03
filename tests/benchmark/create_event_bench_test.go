package eventbus_test

import (
	"testing"

	"eventBus/eventbus"
)

func BenchmarkCreateEvent(b *testing.B) {
	bus := eventbus.NewEventBus(8)
	defer bus.Close()
	_ = bus.CreateTopic("ce")

	var sink int
	b.ReportAllocs()

	for b.Loop() {
		event, err := bus.CreateEvent("E", "ce", "bench")
		if err != nil {
			b.Fatal(err)
		}
		sink += len(event.Metadata().TraceID())
	}
	_ = sink
}
