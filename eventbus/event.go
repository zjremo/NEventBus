// Package eventbus: local eventbus
package eventbus

type Topic string

type MetaData struct {
    TraceID string
    RequestID string
    Source string
}

type Event struct {
    Type string
    Topics []Topic
    MetaData *MetaData
    PayLoad []byte
}
