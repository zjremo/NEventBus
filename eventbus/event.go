// Package eventbus: local eventbus
package eventbus

type Topic string

type Metadata struct {
    TraceID string
    RequestID string
    Source string
}

type Event struct {
    Type string
    Topic Topic
    Metadata *Metadata
    Payload any
}
