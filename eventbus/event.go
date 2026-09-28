// Package eventbus: local eventbus
package eventbus

type Topic string

type metadata struct {
	TraceID   string
	RequestID string
	Source    string
}

type event struct {
	Type     string
	Topic    Topic
	Metadata *metadata
}
