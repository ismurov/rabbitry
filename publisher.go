package rabbitry

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/streadway/amqp"
)

// SyncPublisher is generic helper for creating and serving exchange/queue publisher
// and concurrent publishing from different goroutines into exchange/queue. It supports
// automatic Client reconnection. It use one at time synchronous publishing with confirmation.
// This approach was used because the returned publication does not have delivery tags for
// explicit comparison. If publishing bandwidth is an issue use several publishers with one
// Client.
type SyncPublisher struct {
	logger Logger
	c      Channeler
	pubs   chan *publishing

	// publisher status
	running uint32 // <- atomic (1 - running, 2 - stopped)
	isReady atomicBool
	stopCh  chan struct{}
}

// NewSyncPublisher creates new instance of SyncPublisher. If logger passed in is nil,
// it will use NopLogger.
func NewSyncPublisher(c Channeler, logger Logger) *SyncPublisher {
	if logger == nil {
		logger = NopLogger
	}
	return &SyncPublisher{
		logger: logger,
		c:      c,
		pubs:   make(chan *publishing),
		stopCh: make(chan struct{}),
	}
}

// IsReady returns true if publisher is ready to accept publications for sending.
func (p *SyncPublisher) IsReady() bool {
	return p.isReady.isSet()
}

// Start starts the sync publisher and blocks until the provided context is closed.
func (p *SyncPublisher) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapUint32(&p.running, 0, 1) {
		return errors.New("publisher has already started once")
	}
	defer func() {
		close(p.stopCh)
		atomic.StoreUint32(&p.running, 2)
	}()

	for {
		ch, err := p.c.Channel(ctx)
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

		pubErr := p.publisher(ctx, ch)

		// If the connection channel is already closed, then skip the error from Close method.
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			p.logger(logf("could not close connection channel: %v", err))
		}

		if pubErr != nil {
			if !errors.Is(pubErr, context.Canceled) {
				return pubErr
			}
			return nil
		}
	}
}

// publisher starts a working publisher. The passed amqp.Channel must be without
// any configuration set.
func (p *SyncPublisher) publisher(ctx context.Context, c *amqp.Channel) error {
	// Enable publisher confirms for this channel.
	if err := c.Confirm(false); err != nil {
		return fmt.Errorf("publisher confirms not supported: %w", err)
	}

	p.isReady.setTrue()
	defer p.isReady.setFalse()

	// Uses buffered channels to avoid blocking.
	closeChan := c.NotifyClose(make(chan *amqp.Error, 3))
	returnChan := c.NotifyReturn(make(chan amqp.Return, 3))
	confirmChan := c.NotifyPublish(make(chan amqp.Confirmation, 3))

	// Publishing delivery tags and their corresponding confirmations start at 1.
	var deliveryTag uint64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case closeErr, ok := <-closeChan:
			if !ok {
				p.logger("publisher notify close channel is closed")
				return nil // -> open a new channel
			}
			p.logger(logf("publisher connection channel is closed: %s", closeErr))
			return nil // -> open a new channel

		case r, ok := <-returnChan:
			if !ok {
				p.logger("publisher notify return channel is closed")
				return nil // -> open a new channel
			}
			p.logger(logf(
				"publisher received an unexpected undeliverable message: reason=%d description=%q exchange=%q routingKey=%q",
				r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey,
			))

		case c, ok := <-confirmChan:
			if !ok {
				p.logger("publisher notify confirm channel is closed")
				return nil // -> open a new channel
			}
			// If the delivery tag matches the expected one, it can be a confirmation
			// of the returned publication or already published but canceled by the
			// sender.
			if c.DeliveryTag != deliveryTag {
				p.logger(logf(
					"publisher received an unexpected confirmation: deliveryTag=%d ack=%v currentDeliveryTag=%d",
					c.DeliveryTag, c.Ack, deliveryTag,
				))
			}

		case pub, ok := <-p.pubs:
			if !ok {
				return errors.New("publishing channel is closed")
			}

			// sending publishing
			if err := c.Publish(*pub.exchange, *pub.key, pub.mandatory, pub.immediate, *pub.msg); err != nil {
				pub.errCh <- fmt.Errorf("failed to publish: %w", err)

				if errors.Is(err, amqp.ErrClosed) {
					return nil // -> open a new channel
				}
				continue
			}

			// After successful sending, increase the value of the delivery tag.
			// Package `github.com/streadway/amqp` doesn't expose this value to
			// the outside.
			deliveryTag++

			// Waiting publishing confirmation and send delivery status
			// to publishing error channel.
			isChannelClosed := p.waitConfirm(ctx, deliveryTag, closeChan, returnChan, confirmChan, pub)
			if isChannelClosed {
				return nil // -> open a new channel
			}
		}
	}
}

// waitConfirm listens to all sources of receiving a response about the delivery
// of a message and also handles cancellation by context. After the delivery status
// is resolved, it will be sent to publishing error channel.
func (p *SyncPublisher) waitConfirm(
	ctx context.Context,
	deliveryTag uint64,
	closeChan chan *amqp.Error,
	returnChan chan amqp.Return,
	confirmChan chan amqp.Confirmation,
	pub *publishing,
) (isChannelClosed bool) {
	var pubErr error
	defer func() {
		select {
		case pub.errCh <- pubErr:
		default:
		}
	}()

	for {
		select {
		case <-ctx.Done():
			pubErr = ErrAborted
			return

		case <-pub.ctx.Done():
			pubErr = pub.ctx.Err()
			return

		case er, ok := <-closeChan:
			isChannelClosed = true
			if !ok {
				pubErr = ErrAborted
				return
			}
			pubErr = er
			return

		case r, ok := <-returnChan:
			if !ok {
				pubErr = ErrAborted
				return
			}

			if r.Exchange != *pub.exchange || r.RoutingKey != *pub.key {
				// Received a return of a previous publishing. Log it and continue to wait.
				p.logger(logf(
					"publisher received an unexpected undeliverable message: reason=%d description=%q exchange=%q routingKey=%q",
					r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey,
				))
				continue
			}

			pubErr = fmt.Errorf(
				"failed to deliver message to exchange/queue (returned): %w",
				&amqp.Error{Code: int(r.ReplyCode), Reason: r.ReplyText, Server: true},
			)
			return

		case c, ok := <-confirmChan:
			if !ok {
				pubErr = ErrAborted
				return
			}

			// Received a confirmation of a previous publishing. Log it and continue to wait.
			if c.DeliveryTag != deliveryTag {
				// If the delivery tag matches the previous expected one,
				// it can be a confirmation of the returned publication or
				// already published but canceled by the sender.
				if c.DeliveryTag != deliveryTag-1 {
					p.logger(logf(
						"publisher received an unexpected confirmation: deliveryTag=%d ack=%v currentDeliveryTag=%d",
						c.DeliveryTag, c.Ack, deliveryTag,
					))
				}
				continue
			}

			if c.Ack {
				return // success
			}
			pubErr = ErrNack
			return
		}
	}
}

// sizeof: 56
type publishing struct {
	ctx   context.Context
	errCh chan error

	// amqp data.
	exchange  *string
	key       *string
	mandatory bool
	immediate bool
	msg       *amqp.Publishing
}

// Publish publishes the data to the exchange/queue and waits for confirmation.
// It use one at time synchronous publishing.If publishing bandwidth is an issue
// use several SyncPublisher with one Client.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func (p *SyncPublisher) Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pub := publishing{
		ctx:       ctx,
		errCh:     make(chan error, 1), // size 1 - to avoid blocking in the publisher.
		exchange:  &exchange,
		key:       &key,
		mandatory: mandatory,
		immediate: immediate,
		msg:       &msg,
	}

	// sending a request
	select {
	case <-p.stopCh:
		return ErrPublisherStopped
	case <-pub.ctx.Done():
		return ctx.Err()
	case p.pubs <- &pub:
	}

	// waiting for response
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for a response aborted by context: %w", ctx.Err())
	case err := <-pub.errCh:
		return err
	}
}
