package eventbus_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"eventBus/eventbus"
)

func BenchmarkFanOutThroughput(b *testing.B) {
	for _, nSubs := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("subs=%d", nSubs), func(b *testing.B) {
			topic := eventbus.Topic(fmt.Sprintf("fanout-%d", nSubs))
			bus := eventbus.NewEventBus(256)
			defer bus.Close()
			_ = bus.CreateTopic(topic)

			var calls atomic.Int64
			handler := func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
				calls.Add(1)
				return eventbus.NewResultOK(nil)
			}
			for range nSubs {
				if _, err := bus.Subscribe(topic, handler, false, eventbus.SubParallel, 0); err != nil {
					b.Fatal(err)
				}
			}

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				event, _ := bus.CreateEvent("E", topic, "bench")
				futures, err := bus.PublishEvent(ctx, event)
				if err != nil {
					b.Fatal(err)
				}
				waitAll(futures)
			}
			b.StopTimer()
			if got := calls.Load(); got != int64(b.N)*int64(nSubs) {
				b.Fatalf("calls=%d want=%d", got, int64(b.N)*int64(nSubs))
			}
		})
	}
}
