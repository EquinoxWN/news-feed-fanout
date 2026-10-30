// Package bus is the message log between the post service and the fan-out
// workers. Kafka implements it in deployment; Memory implements the same
// contract in-process (partitions, consumer groups, committed offsets).
//
// Delivery is at least once: a message's offset is committed only after its
// handler returns nil, so a crash or handler error means it is delivered again.
package bus

import (
	"context"
	"errors"
)

// Message is one record read from a topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Handler processes one message; a non-nil error stops the consumer without committing it.
type Handler func(ctx context.Context, m Message) error

// Bus publishes to and consumes from partitioned topics.
type Bus interface {
	// Publish appends a record; records with the same key land in the same partition, in order.
	Publish(ctx context.Context, topic string, key, value []byte) error
	// Consume runs workers members of group on topic until ctx ends (nil) or a handler fails.
	Consume(ctx context.Context, topic, group string, workers int, h Handler) error
}

// ErrGroupBusy is returned when a consumer group is already running in this process.
var ErrGroupBusy = errors.New("bus: consumer group already running")

// ErrArgs is returned for empty topic or group names and non-positive worker counts.
var ErrArgs = errors.New("bus: invalid arguments")

// checkArgs validates Consume arguments.
func checkArgs(topic, group string, workers int, h Handler) error {
	if topic == "" || group == "" || workers < 1 || workers > 1024 || h == nil {
		return ErrArgs
	}
	return nil
}
