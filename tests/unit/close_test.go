package eventbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"eventBus/eventbus"
)

func TestOperationsAfterClose(t *testing.T) {
	bus := eventbus.NewEventBus(8)
	bus.Close()

	if err := bus.CreateTopic("t"); !errors.Is(err, eventbus.ErrEventBusClosed) {
		t.Fatalf("CreateTopic: want ErrEventBusClosed, got %v", err)
	}
	if _, err := bus.ListAllTopics(); !errors.Is(err, eventbus.ErrEventBusClosed) {
		t.Fatalf("ListAllTopics: want ErrEventBusClosed, got %v", err)
	}
	if _, err := bus.Subscribe("t", noopHandler, false, eventbus.SubParallel, 0); !errors.Is(err, eventbus.ErrEventBusClosed) {
		t.Fatalf("Subscribe: want ErrEventBusClosed, got %v", err)
	}
	if _, err := bus.CreateEvent("E", "t", "ut"); !errors.Is(err, eventbus.ErrEventBusClosed) {
		t.Fatalf("CreateEvent: want ErrEventBusClosed, got %v", err)
	}
	if _, err := bus.PublishEvent(context.Background(), nil); !errors.Is(err, eventbus.ErrEventBusClosed) {
		t.Fatalf("PublishEvent: want ErrEventBusClosed, got %v", err)
	}

	bus.Close()
}

func TestCloseWithParallelAndSerialSubs(t *testing.T) {
	bus := eventbus.NewEventBus(16)
	mustCreateTopic(t, bus, "close-me")

	_, err := bus.Subscribe("close-me", noopHandler, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bus.Subscribe("close-me", noopHandler, false, eventbus.SubSerial, 0)
	if err != nil {
		t.Fatal(err)
	}

	bus.Close()
}

func TestCloseDrainsInFlight(t *testing.T) {
	bus := eventbus.NewEventBus(16)
	mustCreateTopic(t, bus, "drain")

	started := make(chan struct{})
	sub, err := bus.Subscribe("drain", func(ctx context.Context, event *eventbus.Event) *eventbus.Result {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return eventbus.NewResultOK("done")
	}, false, eventbus.SubParallel, 0)
	if err != nil {
		t.Fatal(err)
	}

	event, _ := bus.CreateEvent("E", "drain", "ut")
	futures, err := bus.PublishEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	done := make(chan struct{})
	go func() {
		bus.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return; expected drain of in-flight handler")
	}

	res := futures[sub.ID()].Wait()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Data != "done" {
		t.Fatalf("want drained result, got %v", res.Data)
	}
}
