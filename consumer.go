package rabbitry

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/streadway/amqp"
)

// Consumer is generic helper for creating and serving exchange/queue consumer
// and consume delivering messages. It supports automatic Client reconnection.
type Consumer struct {
	logger Logger
	c      Channeler
}

// NewConsumer creates new instance of Consumer. If logger passed in is nil,
// it will use NopLogger.
func NewConsumer(c Channeler, logger Logger) *Consumer {
	if logger == nil {
		logger = NopLogger
	}
	return &Consumer{
		logger: logger,
		c:      c,
	}
}

// StartDeliveringFunc is a function to open and configure delivery channel.
// The function should return explicit amqp errors for handling reconnection in
// Consumer. Or will be unexpected behavior with termination consumer loop with
// error.
type StartDeliveringFunc = func(*amqp.Channel) (<-chan amqp.Delivery, error)

// StartLoop starts the consumer loop, calls once start function on each new
// connection channel for configuration delivery channel and and calls cb on
// each received delivery.
//
// StartLoop blocks until the provided context is closed.
func (c *Consumer) StartLoop(ctx context.Context, start StartDeliveringFunc, cb func(*amqp.Delivery)) error {
	for {
		ch, err := c.c.Channel(ctx)
		if err != nil {
			switch {
			case errors.Is(err, amqp.ErrClosed):
				continue
			case errors.Is(err, context.Canceled):
				return nil
			default:
				return fmt.Errorf("failed to open connection channel: %w", err)
			}
		}

		cErr := c.consumer(ctx, ch, start, cb)

		// If the connection channel is already closed, then skip the error from Close method.
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			c.logger(logf("could not close connection channel: %v", err))
		}

		if cErr != nil {
			if !errors.Is(cErr, context.Canceled) {
				return cErr
			}
			return nil
		}
	}
}

// consumer starts a working consumer. The passed amqp.Channel must be without
// any configuration set.
func (c *Consumer) consumer(ctx context.Context, ch *amqp.Channel, start StartDeliveringFunc, cb func(*amqp.Delivery)) error {
	deliveries, err := start(ch)
	if err != nil {
		if !errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("failed to start delivering: %w", err)
		}
		return nil // -> open a new channel
	}

	cb = c.panicSafeCallback(cb)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case d, ok := <-deliveries:
			if !ok {
				return nil // -> open a new channel
			}
			cb(&d)
		}
	}
}

// panicSafeCallback is panic safe wrapper around callback function.
// For logging panic used consumer logger.
func (c *Consumer) panicSafeCallback(cb func(*amqp.Delivery)) func(*amqp.Delivery) {
	return func(d *amqp.Delivery) {
		defer func() {
			if err := recover(); err != nil {
				const size = 64 << 10 // 64 KiB.
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				c.logger(logf("panic in consumer callback: %v\n%s", err, buf))
			}
		}()

		cb(d)
	}
}
