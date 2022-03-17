package rabbitry

// stringError is a trivial implementation of error.
type stringError string

func (e stringError) Error() string { return string(e) }

const (
	// ErrInvalidConfig means the provided config is not valid.
	ErrInvalidConfig = stringError("invalid config")

	// ErrClientClosed means the client is already closed.
	ErrClientClosed = stringError("client closed")

	// ErrPublisherStopped means the publisher is already stopped.
	ErrPublisherStopped = stringError("publisher stopped")

	// ErrTxClosed is returned by any operation that is performed on a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = stringError("transaction has already been committed or rolled back")

	// ErrAborted means that the executed operation has been aborted.
	ErrAborted = stringError("execution aborted")

	// ErrNack means that the operation has been confirmed with negative acknowledge.
	ErrNack = stringError("negative acknowledge")
)
