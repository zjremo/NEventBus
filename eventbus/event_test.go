package eventbus

import (
	"fmt"
	"log"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/json-iterator/go"
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
    metadata := &MetaData{
        TraceID: node.Generate().Base32(),
        RequestID: node.Generate().Base32(),
        Source: "jrz",
    }

    // 2. build PayLoad
    user := &User{
        Name: "jrz",
        Age: 22,
        Address: "Beijing",
    }

    data, err := jsoniter.Marshal(user)
    if err != nil {
        log.Fatalf("jsoniter Marshal failed, err: %v\n", err)
    }

    // 3. build event
    event := &Event {
        Type: "user:create",
        Topics: []Topic{"1", "2", "3"},
        MetaData: metadata,
        PayLoad: data,
    }

    fmt.Println(event)
}
