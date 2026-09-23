package eventbus

import (
	"log"
	"testing"

	"github.com/bwmarrin/snowflake"
)

type User struct {
    Name string
    Age int64
    Address string
}

func TestEvent(t *testing.T) {
    // 1. build MetaData
    node, err := snowflake.NewNode(1)
    if err != nil {
        log.Fatalf("snowflake produce Node error: %v\n", err)
    }
    metadata := &Metadata{
        TraceID: node.Generate().Base64(),
        RequestID: node.Generate().Base64(),
        Source: "jrz",
    }

    // 2. build Payload
    user := &User{
        Name: "jrz",
        Age: 22,
        Address: "Beijing",
    }
    
    log.Printf("user: %#v\n", user)

    // 3. build event
    event := &Event {
        Type: "user:create",
        Topic: "1",
        Metadata: metadata,
        Payload: user,
    }

    log.Printf("event: %#v\n", event)
    log.Printf("Payload: %#v\n", event.Payload)
}
