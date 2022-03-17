package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// TransferFunc is a function that provides a one-way transfer of data
// from src connection to dst connection.
type TransferFunc func(ctx context.Context, dst, src net.Conn)

// DefaultTransferFunc is a one way data transfer function without any logic.
func DefaultTransferFunc(_ context.Context, dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()

	io.Copy(dst, src) //nolint:errcheck // Any error will just close the connections.
}

// TCPProxy it a service to emulate any unexpected tcp proxy behavior.
type TCPProxy struct {
	ip       string
	port     string
	listener net.Listener
	addr     string
	to       TransferFunc
	from     TransferFunc

	running uint32 // <- atomic (1 - running, 2 - stopped)

	// All logger calls to must be made through logf method for thread safety.
	lmu    sync.RWMutex
	logger func(msg string)
}

// NewTCPProxy creates a new TCPProxy instance with the creation of a network listener
// on the specified port. If the port is empty, it will chosen by local system.
// Transfer functions may be a nil, then it will be used DefaultTransferFunc as default.
func NewTCPProxy(ctx context.Context, port, addr string, to, from TransferFunc) (*TCPProxy, error) {
	if to == nil {
		to = DefaultTransferFunc
	}
	if from == nil {
		from = DefaultTransferFunc
	}

	// First, try establishing a test connection. This ensures that it can reach
	// the target server.
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to establish test connection to %s: %w", addr, err)
	}
	if err := conn.Close(); err != nil {
		return nil, fmt.Errorf("failed to close test connection: %w", err)
	}

	// Second, create a network listener so that the connection is ready when we return.
	// This ensures that it can accept requests.
	laddr := net.JoinHostPort("", port)
	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener on %s: %w", laddr, err)
	}

	// Create a default logger, in most cases this should be enough.
	logger := log.New(os.Stderr, "[rabbitry.e2e.TCPProxy] ", log.LstdFlags|log.Lmsgprefix)

	return &TCPProxy{
		ip:       listener.Addr().(*net.TCPAddr).IP.String(),
		port:     strconv.Itoa(listener.Addr().(*net.TCPAddr).Port),
		listener: listener,
		addr:     addr,
		to:       to,
		from:     from,
		logger:   func(msg string) { logger.Output(3, msg) }, //nolint:errcheck // not critical.
	}, nil
}

// SetLogger provide ability for replace default logger. To correctly collect
// the stack trace, you must use a calldepth with correction to 1.
func (p *TCPProxy) SetLogger(f func(msg string)) {
	p.lmu.Lock()
	p.logger = f
	p.lmu.Unlock()
}

// logf formats a message and calls the logger.
func (p *TCPProxy) logf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)

	p.lmu.RLock()
	p.logger(msg)
	p.lmu.RUnlock()
}

// Addr returns the proxy server's listening address (ip + port).
func (p *TCPProxy) Addr() string {
	return net.JoinHostPort(p.ip, p.port)
}

// IP returns the proxy server's listening IP.
func (p *TCPProxy) IP() string {
	return p.ip
}

// Port returns the proxy server's listening port.
func (p *TCPProxy) Port() string {
	return p.port
}

// TargetAddr returns the target server's address.
func (p *TCPProxy) TargetAddr() string {
	return p.addr
}

// Serve starts the proxy server and blocks until the provided context is closed.
func (p *TCPProxy) Serve(ctx context.Context) (err error) {
	if !atomic.CompareAndSwapUint32(&p.running, 0, 1) {
		return errors.New("tcp proxy has already started once")
	}
	defer atomic.StoreUint32(&p.running, 2)

	l := &onceCloseListener{Listener: p.listener}
	defer l.Close()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Spawn a goroutine that listens for context closure. When the context is
	// closed, the server is stopped.
	errCh := make(chan error, 1)
	go func() {
		<-subCtx.Done()
		errCh <- l.Close()
	}()

	var (
		tempDelay time.Duration // how long to sleep on accept failure
		sErr      error
	)
LOOP:
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-subCtx.Done():
				break LOOP
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond // min sleep delay
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max { // max sleep delay
					tempDelay = max
				}

				p.logf("connection accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			sErr = err
			break
		}
		tempDelay = 0

		go p.handleConn(ctx, conn)
	}

	// Return any errors that happened during shutdown.
	select {
	case er := <-errCh:
		if er != nil {
			sErr = fmt.Errorf("listener closing error: %w (original error: %v)", er, sErr)
		}
	default:
	}

	return sErr
}

// handleConn establishes a proxy connection and handles data transfer for it.
func (p *TCPProxy) handleConn(ctx context.Context, src net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			p.logf("panic serving %v connection: %v", src.RemoteAddr(), r)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var dialer net.Dialer
	dst, err := dialer.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		p.logf("failed to establish connection to %s: %v", p.addr, err)
		return
	}

	src = &onceCloseConn{Conn: src}
	dst = &onceCloseConn{Conn: dst}

	go func() {
		<-ctx.Done()
		src.Close()
		dst.Close()
	}()

	go p.transfer(ctx, src, dst, p.from)
	p.transfer(ctx, dst, src, p.to)
}

func (p *TCPProxy) transfer(ctx context.Context, dst, src net.Conn, f TransferFunc) {
	defer dst.Close()
	defer src.Close()
	f(ctx, dst, src)
}

// onceCloseListener wraps a net.Listener, protecting it from
// multiple Close calls.
type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *onceCloseListener) close() { oc.closeErr = oc.Listener.Close() }

// onceCloseConn wraps a net.Conn, protecting it from
// multiple Close calls.
type onceCloseConn struct {
	net.Conn
	once     sync.Once
	closeErr error
}

func (oc *onceCloseConn) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *onceCloseConn) close() { oc.closeErr = oc.Conn.Close() }
