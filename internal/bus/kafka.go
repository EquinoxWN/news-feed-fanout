package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka is a Bus backed by Apache Kafka through franz-go.
type Kafka struct {
	brokers  []string
	producer *kgo.Client
}

// NewKafka connects a producer to the given seed brokers.
func NewKafka(brokers []string) (*Kafka, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%w: no brokers", ErrArgs)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return nil, fmt.Errorf("bus: kafka producer: %w", err)
	}
	return &Kafka{brokers: brokers, producer: cl}, nil
}

// EnsureTopic creates topic with the given partitions (replication 1) unless it exists.
func (k *Kafka) EnsureTopic(ctx context.Context, topicName string, partitions int32) error {
	resp, err := kadm.NewClient(k.producer).CreateTopic(ctx, partitions, 1, nil, topicName)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("bus: create topic %s: %w", topicName, err)
	}
	return nil
}

// Publish produces one record and waits for the broker acknowledgement.
func (k *Kafka) Publish(ctx context.Context, topicName string, key, value []byte) error {
	if topicName == "" {
		return ErrArgs
	}
	return k.producer.ProduceSync(ctx, &kgo.Record{Topic: topicName, Key: key, Value: value}).FirstErr()
}

// Consume starts workers group members, each its own client, and commits after each handled poll.
func (k *Kafka) Consume(ctx context.Context, topicName, groupName string, workers int, h Handler) error {
	if err := checkArgs(topicName, groupName, workers, h); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if err := k.member(ctx, topicName, groupName, h); err != nil {
				errc <- err
				cancel()
			}
		})
	}
	wg.Wait()
	close(errc)
	return <-errc
}

// member is one consumer-group client; offsets are committed only after every record of a poll is handled.
func (k *Kafka) member(ctx context.Context, topicName, groupName string, h Handler) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumerGroup(groupName),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("bus: kafka consumer: %w", err)
	}
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.Canceled) {
				return fmt.Errorf("bus: fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
			}
		}
		var herr error
		fetches.EachRecord(func(r *kgo.Record) {
			if herr == nil {
				herr = h(ctx, Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value})
			}
		})
		if herr != nil {
			return fmt.Errorf("bus: handler: %w", herr)
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			return fmt.Errorf("bus: commit: %w", err)
		}
		cl.AllowRebalance()
	}
}

// Close flushes and closes the producer.
func (k *Kafka) Close() { k.producer.Close() }
