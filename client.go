package rabbitry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/streadway/amqp"
)

// Client is a thread-safe wrapper around amqp.Connection that supports
// automatic reconnection.
type Client struct {
	logger Logger

	// connection factory
	factory         *ConnectionFactory
	stopFactory     func()
	doneFactoryChan chan struct{}

	// connection state
	connected       atomicBool
	channelRequests chan *channelRequest

	// client termination flow
	closed     atomicBool
	destructor sync.Once
	doneChan   chan struct{}
}

// New returns new instance of Client with RabbitMQ server url set.
//
// If cfg passed in is nil, it will fallback to use default configuration.
func New(ctx context.Context, url string, cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = defaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	cfg.fallbackDefaults()

	cf, err := NewConnectionFactory(ctx, url, cfg)
	if err != nil {
		return nil, fmt.Errorf("creation connection factory: %w", err)
	}

	// connection factory context
	cfCtx, cfCtxCancel := context.WithCancel(context.Background())

	c := &Client{
		logger:          cfg.Logger,
		factory:         cf,
		stopFactory:     cfCtxCancel,
		doneFactoryChan: make(chan struct{}),
		channelRequests: make(chan *channelRequest),
		doneChan:        make(chan struct{}),
	}

	go c.startFactory(cfCtx)

	return c, err
}

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

// IsConnected returns true if the client has an active connection to the RabbitMQ
// server, false otherwise.
func (c *Client) IsConnected() bool {
	return c.connected.isSet()
}

// IsClosed returns true if the client is marked as closed, false otherwise.
func (c *Client) IsClosed() bool {
	return c.closed.isSet()
}

// Close terminates the client and closes the active connection.
func (c *Client) Close() error {
	if c.closed.isSet() {
		return nil
	}
	defer c.closed.setTrue()

	c.destructor.Do(func() {
		// reject pending connection channel requests.
		close(c.doneChan)

		// shutdown the connection factory
		c.stopFactory()

		// wait connection factory termination
		<-c.doneFactoryChan
	})

	return nil
}

// startFactory starts the connection factory and blocks until the client is
// stopped with Close method.
func (c *Client) startFactory(ctx context.Context) {
	if err := c.factory.StartLoop(ctx, func(conn *amqp.Connection) error {
		return c.serve(ctx, conn)
	}); err != nil && !errors.Is(err, context.Canceled) {
		c.logger(logf(
			"connection factory stopped with an unexpected error: %v", err,
		))
	}

	if c.doneFactoryChan != nil {
		select {
		case <-c.doneFactoryChan:
		default:
			close(c.doneFactoryChan)
		}
	}
}

// serve watches for connection activity and shares connection channels.
func (c *Client) serve(ctx context.Context, conn *amqp.Connection) error {
	c.connected.setTrue()
	defer c.connected.setFalse()

	// uses buffered channels to avoid blocking.
	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))
	blockChan := conn.NotifyBlocked(make(chan amqp.Blocking, 1))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case closeErr, ok := <-closeChan:
			if !ok {
				c.logger("notify close channel is closed")
				return nil // -> do a reconnect
			}
			c.logger(logf("connection is closed: %s", closeErr))
			return nil // -> do a reconnect

		case block, ok := <-blockChan:
			if !ok {
				c.logger("notify block channel is closed")
				return nil // -> do a reconnect
			}
			if !block.Active {
				c.logger(logf("connection is blocked; reason: %s", block.Reason))
			} else {
				c.logger("connection is unblocked")
			}

		case req, ok := <-c.channelRequests:
			if !ok {
				return errors.New("channel requests channel is closed")
			}

			ch, chErr := conn.Channel()

			// sending a response on channel request
			select {
			case req.resp <- &channelResponse{ch: ch, err: chErr}:
			default:
			}

			if chErr != nil {
				if errors.Is(chErr, amqp.ErrClosed) {
					return nil // -> do a reconnect
				}

				c.logger(logf("failed to open connection channel: %s", chErr))
			}
		}
	}
}

type channelRequest struct {
	resp chan *channelResponse
}

func newChannelRequest() *channelRequest {
	return &channelRequest{
		resp: make(chan *channelResponse, 1), // size 1 - to avoid blocking.
	}
}

type channelResponse struct {
	ch  *amqp.Channel
	err error
}

// Channeler is an interface for using the Channel method of Client without strict type.
// This interface should implement original logic of Client method. This can be useful when
// you need to grant limited access to a client instance.
type Channeler interface {
	// Channel opens a unique, concurrent server channel to process the bulk
	// of AMQP messages. This method has no timeouts, to avoid long locks, use
	// context.WithDeadline or context.WithTimeout.
	Channel(ctx context.Context) (*amqp.Channel, error)
}

// Channel opens a unique, concurrent server channel to process the bulk of AMQP messages.
//
// This function makes several attempts to handle the reconnect error (maximum 3 attempts)
// and if it failed may returns amqp.ErrClosed error. If the client is already closed, the
// ErrClientClosed error will be returned.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func (c *Client) Channel(ctx context.Context) (*amqp.Channel, error) {
	var (
		ch  *amqp.Channel
		err error
	)
	for i := 0; i < 3; i++ {
		ch, err = c.channel(ctx)
		if err == nil {
			return ch, nil
		}

		if !errors.Is(err, amqp.ErrClosed) {
			break
		}
	}

	return nil, err
}

// channel sends request for opening new connection channel.
func (c *Client) channel(ctx context.Context) (*amqp.Channel, error) {
	if c.closed.isSet() {
		return nil, ErrClientClosed
	}

	req := newChannelRequest()

	// sending a request
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.doneChan:
		return nil, ErrClientClosed
	case c.channelRequests <- req:
	}

	// waiting for response
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.doneChan:
		return nil, ErrClientClosed
	case resp := <-req.resp:
		return resp.ch, resp.err
	}
}

// BeginTx starts a transaction.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func (c *Client) BeginTx(ctx context.Context) (*Tx, error) {
	return BeginTx(ctx, c)
}

// InTx runs the given function f within a transaction.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func (c *Client) InTx(ctx context.Context, f func(*Tx) error) error {
	return InTx(ctx, c, f)
}
