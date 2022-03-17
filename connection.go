package rabbitry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

// Quote from 'github.com/streadway/amqp' library:
//   Connection locale that we expect to always be en_US
//   Even though servers must return it as per the AMQP 0-9-1 spec,
//   we are not aware of it being used other than to satisfy the spec requirements.
const defaultLocale = "en_US"

// ConnectionFactory is a helper for reconnecting to RabbitMQ server.
type ConnectionFactory struct {
	logger Logger
	url    string

	// Connection factory configuration.
	heartbeat           time.Duration
	connectionTimeout   time.Duration
	reconnectionTimeout time.Duration
}

// NewConnectionFactory returns new instance of ConnectionFactory
// with RabbitMQ server url set.
//
// If cfg passed in is nil, it will fallback to use default configuration.
func NewConnectionFactory(ctx context.Context, url string, cfg *Config) (*ConnectionFactory, error) {
	if cfg == nil {
		cfg = defaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	cfg.fallbackDefaults()

	cf := &ConnectionFactory{
		logger:              cfg.Logger,
		url:                 url,
		heartbeat:           cfg.Heartbeat,
		connectionTimeout:   cfg.ConnectionTimeout,
		reconnectionTimeout: cfg.ReconnectionTimeout,
	}

	if err := cf.CheckConnection(ctx); err != nil {
		return nil, fmt.Errorf("failed to establish test connection: %w", err)
	}

	return cf, nil
}

// CheckConnection tries to connect to the RabbitMQ server by connection factory url.
// After successful connection, the connection will be closed.
func (cf *ConnectionFactory) CheckConnection(ctx context.Context) error {
	conn, err := cf.dial(ctx)
	if err != nil {
		return err
	}
	if err = conn.Close(); err != nil {
		return fmt.Errorf("could not close connection: %w", err)
	}
	return nil
}

// Dial connects to the RabbitMQ server by connection factory url.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func (cf *ConnectionFactory) Dial(ctx context.Context) (*amqp.Connection, error) {
	return cf.dialLoop(ctx)
}

// StartLoop starts the connection factory via the cf.Start method and runs
// a callback function with a new connection when a connection is received
// from the channel. The received connection may not be closed inside the
// callback, it is handled at the top level. For to reconnect, the callback
// function must return nil error.
//
// StartLoop blocks until the provided context is closed.
func (cf *ConnectionFactory) StartLoop(ctx context.Context, cb func(*amqp.Connection) error) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	for request := range cf.Start(ctx) {
		conn, ok := <-request
		if !ok {
			select {
			case <-ctx.Done():
				if er := ctx.Err(); er != nil && !errors.Is(er, context.Canceled) {
					return er
				}
				return nil
			default:
				return errors.New("connection channal closed")
			}
		}

		cbErr := cb(conn)

		// If the connection is already closed, then skip the error from Close method.
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			cf.logger(logf("could not close connection: %v", err))
		}

		if cbErr != nil {
			if !errors.Is(cbErr, context.Canceled) {
				return cbErr
			}
			return nil
		}
	}
	return nil
}

// Start starts the connection factory and returns a channel for new connection.
// After receive a connection from the channel of new connection factory tries to establish
// new connection to RabbitMQ servers. The received channel is blocked until the connection
// is successfully established and returned for a new session through that connection channel.
//
// The connection factory can only handle one connection at a time and runs until the provided
// context is closed.
func (cf *ConnectionFactory) Start(ctx context.Context) <-chan (<-chan *amqp.Connection) {
	requests := make(chan (<-chan *amqp.Connection))

	go func() {
		defer close(requests)

		for {
			// new connection channel.
			connCh := make(chan *amqp.Connection)

			// waiting for the start of a new session.
			select {
			case requests <- connCh:
			case <-ctx.Done():
				close(connCh)
				return
			}

			conn, err := cf.dialLoop(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					cf.logger(logf("cannot (re)connect: %v", err))
				}
				close(connCh)
				return
			}

			// sending a new connection to the consumer.
			select {
			case connCh <- conn:
			case <-ctx.Done():
				close(connCh)
				return
			}

			// close the connection channel after the consumer has received
			// a connection.
			close(connCh)
		}
	}()

	return requests
}

// dialLoop tries to establish a connection in an infinite loop. The loop can
// be stopped by canceling provided context. In case of stopping via context,
// the error context.Canceled will be returned.
func (cf *ConnectionFactory) dialLoop(ctx context.Context) (conn *amqp.Connection, err error) {
	timeout := cf.reconnectionTimeout

	var attempt int
	for {
		attempt++

		conn, err = cf.dial(ctx)
		if err == nil {
			return conn, nil
		}

		select {
		case <-ctx.Done():
			return nil, context.Canceled
		default:
		}

		cf.logger(logf(
			"connection error: %v; waiting %v before retrying (attempt: %d)",
			err, timeout, attempt,
		))

		select {
		case <-time.After(timeout):
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}

func (cf *ConnectionFactory) dial(ctx context.Context) (*amqp.Connection, error) {
	return amqp.DialConfig(cf.url, amqp.Config{
		Dial:      contextDialer(ctx, cf.connectionTimeout),
		Heartbeat: cf.heartbeat,
		Locale:    defaultLocale,
	})
}

func contextDialer(ctx context.Context, timeout time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}

		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			// The underlying net.Dial function is not properly reporting context.Canceled
			// errors. Because of this, a string check on the error is performed. There's
			// an open issue for this and it appears it will be fixed eventually but for
			// now we have this check to avoid unnecessary logs.
			// https://github.com/golang/go/issues/36208
			if strings.Contains(err.Error(), "operation was canceled") {
				err = context.Canceled
			}
			return nil, err
		}

		// Heartbeating hasn't started yet, don't stall forever on a dead server.
		// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
		// the deadline is cleared in openComplete.
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}
