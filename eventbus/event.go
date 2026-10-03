// Package eventbus: local eventbus
package eventbus

type Topic string

type Metadata struct {
	traceID   string
	requestID string
	source    string
}

func (m *Metadata) TraceID() string {
	return m.traceID
}

func (m *Metadata) RequestID() string {
	return m.requestID
}

func (m *Metadata) Source() string {
	return m.source
}

type Event struct {
	typeDesc string
	topic    Topic
	metadata *Metadata
}

func (e *Event) TypeDesc() string {
	return e.typeDesc
}

func (e *Event) Topic() Topic {
	return e.topic
}

func (e *Event) Metadata() *Metadata {
	return e.metadata
}
