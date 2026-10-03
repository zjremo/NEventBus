package eventbus_test

import (
	"context"
	"errors"
	"testing"

	"eventBus/eventbus"
)

func TestHandlerPanicParallel(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "panic-p")

	sub, err := bus.Subscribe("panic-p", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		panic("boom")
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "panic-p", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	res := futures[sub.ID()].Wait()
	if !errors.Is(res.Err, eventbus.ErrHandlerPanic) {
		t.Fatalf("want ErrHandlerPanic, got %v", res.Err)
	}

	// 池 worker 应仍可用
	sub2, err := bus.Subscribe("panic-p", noopHandler, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}
	event2, _ := bus.CreateEvent("E", "panic-p", "ut")
	futures2, err := bus.PublishEvent(context.Background(), event2)
	if err != nil {
		t.Fatal(err)
	}
	if res := futures2[sub2.ID()].Wait(); res.Err != nil {
		t.Fatalf("bus should still work after handler panic: %v", res.Err)
	}
}

func TestHandlerPanicSerial(t *testing.T) {
	bus := newTestBus(t)
	mustCreateTopic(t, bus, "panic-s")

	sub, err := bus.Subscribe("panic-s", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		panic("boom")
	}, false, eventbus.SubSerial, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "panic-s", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	res := futures[sub.ID()].Wait()
	if !errors.Is(res.Err, eventbus.ErrHandlerPanic) {
		t.Fatalf("want ErrHandlerPanic, got %v", res.Err)
	}
}
