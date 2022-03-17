package rabbitry

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

// Tx is an in-progress transaction. A transaction must end with a call to
// Commit or Rollback. After a call to Commit or Rollback, all operations
// on the transaction fail with ErrTxClosed.
type Tx struct {
	ch       *amqp.Channel
	closed   bool
	closeErr error
}

// BeginTx starts a transaction.
//
// WARNING: This method has no timeouts, to avoid long locks,
//          use context.WithDeadline or context.WithTimeout.
func BeginTx(ctx context.Context, c Channeler) (*Tx, error) {
	ch, err := c.Channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection channel: %w", err)
	}
	if err := ch.Tx(); err != nil {
		return nil, err
	}
	return &Tx{ch: ch}, nil
}

// Close closes connection channel or returns closing error
// if it was populated after call to Commit or Rollback.
func (tx *Tx) Close() error {
	if tx.closed {
		return tx.closeErr
	}
	tx.closeErr = tx.Close()
	return tx.closeErr
}

// Commit atomically commits all publishings and acknowledgments for a single
// queue and and close connection channel. If an error was returned when closing
// the connection channel, it can be received via Err method.
func (tx *Tx) Commit() (err error) {
	if tx.closed {
		return ErrTxClosed
	}
	err = tx.ch.TxCommit()

	tx.closed = true
	tx.closeErr = tx.ch.Close()
	tx.ch = nil

	return
}

// Rollback atomically rolls back all publishings and acknowledgments for a
// single queue and close connection channel. If an error was returned when
// closing the connection channel, it can be received via Err method.
func (tx *Tx) Rollback() (err error) {
	if tx.closed {
		return ErrTxClosed
	}
	err = tx.ch.TxRollback()

	tx.closed = true
	tx.closeErr = tx.ch.Close()
	tx.ch = nil

	return
}

// Publish publishes the data to the exchange/queue without waits for confirmation.
// All publications will be confirmed by calling Commit.
func (tx *Tx) Publish(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.ch.Publish(exchange, key, mandatory, immediate, msg)
}

// InTx runs the given function f within a transaction.
func InTx(ctx context.Context, c *Client, f func(tx *Tx) error) error {
	tx, err := BeginTx(ctx, c)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}

	if err := f(tx); err != nil {
		if er := tx.Rollback(); er != nil {
			return fmt.Errorf("rolling back transaction: %w (original error: %v)", er, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	if err := tx.Close(); err != nil {
		return fmt.Errorf("closing connection channel: %w", err)
	}
	return nil
}
