//go:build rabbitry_e2e_test

package rabbitry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"

	"github.com/ismurov/rabbitry"
	"github.com/ismurov/rabbitry/e2e"
)

var testInstance *e2e.TestInstance

func TestMain(m *testing.M) {
	var code int
	defer func() { os.Exit(code) }()

	testInstance = e2e.MustTestInstance()
	defer testInstance.MustClose()

	code = m.Run()
}

// connectionURL creates a new vhost suitable for use in testing. It returns
// an connection url to that vhost.
func connectionURL(tb testing.TB) string {
	tb.Helper()

	return testInstance.NewVhost(tb).String()
}

// declareTestQueue declares exclusive queue for test, that will be deleted
// when the connection closes.
func declareTestQueue(ctx context.Context, tb testing.TB, c *rabbitry.Client) (string, *amqp.Channel) {
	tb.Helper()

	ch, err := c.Channel(ctx)
	if err != nil {
		tb.Fatalf("opening connection channel: %v", err)
	}
	tb.Cleanup(func() {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			tb.Errorf("closing connection channel: %v", err)
		}
	})

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		tb.Fatalf("queue declaration: %v", err)
	}

	return q.Name, ch
}

// TestE2E_Client is a test for simple client usage without any corner cases.
// Flow: connect, open a channel, declare a queue.
func TestE2E_Client(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			ch, err := c.Channel(ctx)
			if err != nil {
				t.Errorf("[n=%d] (*rabbitry.Client).Channel(): %s", n, err)
				ctxCancel() // <- Something is wrong, terminate all consumers.
				return
			}
			defer func() {
				if err = ch.Close(); err != nil {
					t.Errorf("[n=%d] (*amqp.Channel).Close(): %s", n, err)
				}
			}()

			name := fmt.Sprintf("queue-e2e-simple-%d", n)
			q, err := ch.QueueDeclare(name, false, true, true, false, nil)
			if err != nil {
				t.Errorf("[n=%d] (*amqp.Channel).QueueDeclare(): %s", n, err)
				return
			}

			_, err = ch.QueueDelete(q.Name, true, true, false)
			if err != nil {
				t.Errorf("[n=%d] (*amqp.Channel).QueueDelete(): %s", n, err)
			}
		}(i + 1)
	}

	wg.Wait()
}

// TestE2E_Client_ChannelReconnect is a test of the functionality of opening
// the connection channel for resistance to repeated connections.
func TestE2E_Client_ChannelReconnect(t *testing.T) {
	t.Parallel()

	vhostURL := testInstance.NewVhost(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Proxy transfer function that reject every second connection.
	var attempts int32
	transferTo := func(ctx context.Context, dst, src net.Conn) {
		if n := atomic.AddInt32(&attempts, 1); n%2 == 1 {
			e2e.DefaultTransferFunc(ctx, dst, src)
		}
	}

	// Create tcp proxy.
	proxy, err := e2e.NewTCPProxy(ctx, "", vhostURL.Host, transferTo, nil)
	if err != nil {
		t.Fatalf("e2e.NewTCPProxy: %v", err)
	}
	proxy.SetLogger(rabbitry.TestLogger(t, true, 1))

	// Spawn a goroutine that serve tcp proxy.
	proxyDone := make(chan struct{})
	go func() {
		if err := proxy.Serve(ctx); err != nil {
			t.Errorf("(*e2e.TCPProxy).Serve(): %v", err)
		}
		ctxCancel()
		proxyDone <- struct{}{}
	}()
	defer func() {
		ctxCancel()
		<-proxyDone
	}()

	// Create client.
	proxyURL := *vhostURL
	proxyURL.Host = proxy.Addr()
	c, err := rabbitry.New(ctx, proxyURL.String(), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, false, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	t.Cleanup(func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	})

	ch, err := c.Channel(ctx)
	if err != nil {
		t.Errorf("(*rabbitry.Client).Channel(): %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Errorf("(*amqp.Channel).Close(): %v", err)
	}

	// attempts:
	//  - connection check    (1)
	//  - connections in loop (2)
	if got, want := atomic.LoadInt32(&attempts), int32(3); got != want {
		t.Errorf("wrong number of connection attempts: got=%d want=%d", got, want)
	}
}

// TestE2E_ConnectionFactory is a test for the functionality of the connection factory
// and resistance to reconnections.
func TestE2E_ConnectionFactory(t *testing.T) {
	t.Parallel()

	vhostURL := testInstance.NewVhost(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Proxy transfer function that reject every second connection.
	var attempts int32
	transferTo := func(ctx context.Context, dst, src net.Conn) {
		if n := atomic.AddInt32(&attempts, 1); n%2 == 1 {
			e2e.DefaultTransferFunc(ctx, dst, src)
		}
	}

	// Create tcp proxy.
	proxy, err := e2e.NewTCPProxy(ctx, "", vhostURL.Host, transferTo, nil)
	if err != nil {
		t.Fatalf("e2e.NewTCPProxy: %v", err)
	}
	proxy.SetLogger(rabbitry.TestLogger(t, true, 1))

	// Spawn a goroutine that serve tcp proxy.
	proxyDone := make(chan struct{})
	go func() {
		if err := proxy.Serve(ctx); err != nil {
			t.Errorf("(*e2e.TCPProxy).Serve(): %v", err)
		}
		ctxCancel()
		proxyDone <- struct{}{}
	}()
	defer func() {
		ctxCancel()
		<-proxyDone
	}()

	// Create connection factory.
	proxyURL := *vhostURL
	proxyURL.Host = proxy.Addr()
	cf, err := rabbitry.NewConnectionFactory(ctx, proxyURL.String(), &rabbitry.Config{
		Logger:              rabbitry.TestLogger(t, false, 0),
		Heartbeat:           500 * time.Millisecond,
		ConnectionTimeout:   2 * time.Second,
		ReconnectionTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Errorf("rabbitry.NewConnectionFactory: %v", err)
		return
	}

	// Do simple dial.
	conn, err := cf.Dial(ctx)
	if err != nil {
		t.Errorf("(*rabbitry.ConnectionFactory).Dial: %v", err)
		return
	}
	if err = conn.Close(); err != nil {
		t.Errorf("(*amqp.Connection).Close(): %s", err)
		return
	}

	loopCtx, loopCtxCancel := context.WithCancel(context.Background())
	defer loopCtxCancel()

	// Do two successful connections in loop.
	var repeat bool
	if err = cf.StartLoop(loopCtx, func(_ *amqp.Connection) error {
		if repeat {
			loopCtxCancel()
		}
		repeat = true
		return nil
	}); err != nil {
		t.Errorf("(*rabbitry.ConnectionFactory).StartLoop: %v", err)
		return
	}

	// attempts:
	//  - connection check    (1)
	//  - simple simple dial  (2)
	//  - connections in loop (4)
	if got, want := atomic.LoadInt32(&attempts), int32(7); got != want {
		t.Errorf("wrong number of connection attempts: got=%d want=%d", got, want)
	}
}

// TestE2E_Tx_Commit is a test for transaction flow with atomically commits
// all publishings and acknowledgments.
func TestE2E_Tx_Commit(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	t.Cleanup(func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	})

	queue, ch := declareTestQueue(ctx, t, c)

	messageCount := 10
	if txErr := c.InTx(ctx, func(tx *rabbitry.Tx) error {
		for i := 0; i < messageCount; i++ {
			n := i + 1
			msgID := strconv.Itoa(n)
			if err := tx.Publish(ctx, "", queue, false, false, amqp.Publishing{
				MessageId:   msgID,
				ContentType: "text/plain",
				Body:        []byte("content on message " + msgID),
			}); err != nil {
				return fmt.Errorf("failed to publish (%d): %w", n, err)
			}
		}
		return nil
	}); txErr != nil {
		t.Fatalf("transaction error: %v", txErr)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != messageCount {
		t.Errorf("not all messages have been published: got=%d want=%d", q.Messages, messageCount)
	}
}

// TestE2E_Tx_Rollback is a test for transaction flow with atomically rolls back
// all publishings and acknowledgments.
func TestE2E_Tx_Rollback(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	t.Cleanup(func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	})

	queue, ch := declareTestQueue(ctx, t, c)

	errRollback := errors.New("test rollback error")

	messageCount := 10
	if txErr := c.InTx(ctx, func(tx *rabbitry.Tx) error {
		for i := 0; i < messageCount; i++ {
			n := i + 1
			msgID := strconv.Itoa(n)
			if err := tx.Publish(ctx, "", queue, false, false, amqp.Publishing{
				MessageId:   msgID,
				ContentType: "text/plain",
				Body:        []byte("content on message " + msgID),
			}); err != nil {
				return fmt.Errorf("failed to publish (%d): %w", n, err)
			}
		}
		return errRollback
	}); !errors.Is(txErr, errRollback) {
		t.Fatalf("unexpected error after rolls back transaction: %v", txErr)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != 0 {
		t.Errorf("after rollback transaction a queue is not empty (messages: %d)", q.Messages)
	}
}

// TestE2E_SyncPublisher is a test for concurrent sending with sync publisher.
func TestE2E_SyncPublisher(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	queue, ch := declareTestQueue(ctx, t, c)

	p := rabbitry.NewSyncPublisher(c, rabbitry.TestLogger(t, true, 0))

	pubCtx, pubCtxCancel := context.WithCancel(ctx)
	defer pubCtxCancel()

	messageCount := 100

	// Sending publications from multiple goroutines at the same time.
	var wg sync.WaitGroup
	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			msgID := strconv.Itoa(n)
			if err := p.Publish(pubCtx, "", queue, false, false, amqp.Publishing{
				MessageId:   msgID,
				ContentType: "text/plain",
				Body:        []byte("content on message " + msgID),
			}); err != nil {
				t.Errorf("[n=%d] (*rabbitry.SyncPublisher).Publish(): %s", n, err)
			}
		}(i + 1)
	}

	go func() {
		// Special case: sending to non-existing-queue.
		var amqpErr *amqp.Error
		if err := p.Publish(pubCtx, "", "non-existing-queue", true, false, amqp.Publishing{
			MessageId:   "msg-return-1",
			ContentType: "text/plain",
			Body:        []byte("content on message msg-return-1"),
		}); !(errors.As(err, &amqpErr) && amqpErr.Code == amqp.NoRoute) {
			t.Errorf("publishing to non-existing-queue returns wrong error: %v", err)
		}

		// Wait to send all publications.
		wg.Wait()
		pubCtxCancel()
	}()

	// Start sync publisher and wait for the sending to be completed.
	if err := p.Start(pubCtx); err != nil {
		t.Errorf("(*rabbitry.SyncPublisher).Start(): %v", err)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != messageCount {
		t.Errorf("not all messages have been published: got=%d want=%d", q.Messages, messageCount)
	}
}

// TestE2E_SyncPublisher_ExchangeNotFound is a test for synchronous publishing
// when exchange does not exist.
func TestE2E_SyncPublisher_ExchangeNotFound(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	t.Cleanup(func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	})

	queue, _ := declareTestQueue(ctx, t, c)

	pubCtx, pubCtxCancel := context.WithCancel(ctx)
	defer pubCtxCancel()

	p := rabbitry.NewSyncPublisher(c, rabbitry.TestLogger(t, true, 0))
	pubDone := make(chan struct{})
	go func() {
		if err := p.Start(pubCtx); err != nil {
			t.Errorf("(*rabbitry.SyncPublisher).Start(): %v", err)
		}
		pubCtxCancel()
		pubDone <- struct{}{}
	}()

	messageCount := 10

	// Sending publications from multiple goroutines at the same time.
	var wg sync.WaitGroup
	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			msgID := strconv.Itoa(n)
			var amqpErr *amqp.Error
			if err := p.Publish(pubCtx, "non-existing-exchange", queue, false, false, amqp.Publishing{
				MessageId:   msgID,
				ContentType: "text/plain",
				Body:        []byte("content on message " + msgID),
			}); !(errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound) {
				t.Errorf("unexpected publish error: %v", err)
			}
		}(i + 1)
	}

	// Wait to send all publications.
	wg.Wait()
	pubCtxCancel()
	<-pubDone
}

// findMagicIndex is a handler for finding the beginning of a magic sequence
// in a byte slice. Returns an index if part of Magic is detected, or -1 if
// magic does not exist. The second boolean value means that whole magic is
// found.
func findMagicIndex(b, magic []byte) (int, bool) {
	if i := bytes.Index(b, magic); i != -1 {
		return i, true // found full magic
	}

	n := 1
	for n < len(magic) {
		j := len(magic) - n
		i := len(b) - j
		n++
		if i < 0 {
			continue
		}
		if bytes.Equal(b[i:], magic[:j]) {
			return i, false
		}
	}
	return -1, false
}

// transferBeforeMagic is transfer function that breaks the connection
// if a sequence of magic bytes is received. It is like some explode.
func transferBeforeMagic(tb testing.TB, magic []byte) e2e.TransferFunc {
	tb.Helper()

	if len(magic) == 0 {
		tb.Fatalf("magic sequence is empty")
	}
	if len(magic) > 10 {
		tb.Fatalf("magic sequence is too big")
	}

	return func(_ context.Context, dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		observed := make([]byte, 0, len(magic))
		for {
			nr, er := src.Read(buf)
			if nr > 0 {
				if len(observed) > 0 {
					// The previous iteration contained the part of magic sequence.
					observed = append(observed[:0], buf[:cap(observed)-len(observed)]...)
					if bytes.Equal(observed, magic) {
						return
					}
					observed = observed[:0]
				}
				index, got := findMagicIndex(buf, magic)
				if got {
					return
				}
				if index != -1 {
					// Got the part of magic sequence, save it.
					observed = observed[:cap(observed)]
					copy(observed, buf[index:])
				}

				nw, ew := dst.Write(buf[0:nr])
				if nw < 0 || nr < nw || ew != nil {
					return
				}
			}
			if er != nil {
				return
			}
		}
	}
}

// TestE2E_SyncPublisher_Disconnect is a test for synchronous publishing
// when the connection is broken after publication.
func TestE2E_SyncPublisher_Disconnect(t *testing.T) {
	t.Parallel()

	vhostURL := testInstance.NewVhost(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	magicSequence := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0x98, 0x76, 0x54, 0x32, 0x10}

	// Create tcp proxy with transfer function that breaks the connection
	// if a sequence of magic bytes is received.
	proxy, err := e2e.NewTCPProxy(ctx, "", vhostURL.Host, transferBeforeMagic(t, magicSequence), nil)
	if err != nil {
		t.Fatalf("e2e.NewTCPProxy: %v", err)
	}
	proxy.SetLogger(rabbitry.TestLogger(t, true, 1))

	// Spawn a goroutine that serve tcp proxy.
	proxyDone := make(chan struct{})
	go func() {
		if err := proxy.Serve(ctx); err != nil {
			t.Errorf("(*e2e.TCPProxy).Serve(): %v", err)
		}
		ctxCancel()
		proxyDone <- struct{}{}
	}()
	defer func() {
		ctxCancel()
		<-proxyDone
	}()

	// Create client.
	proxyURL := *vhostURL
	proxyURL.Host = proxy.Addr()
	c, err := rabbitry.New(ctx, proxyURL.String(), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, false, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	// Open connection channel.
	ch, err := c.Channel(ctx)
	if err != nil {
		t.Fatalf("(*rabbitry.Client).Channel(): %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("(*amqp.Channel).Close: %v", err)
		}
	}()

	// Declares exclusive queue for test, that will be deleted
	// when the connection closes.
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("(*amqp.Channel)QueueDeclare(): %v", err)
	}

	pubCtx, pubCtxCancel := context.WithCancel(ctx)
	defer pubCtxCancel()

	// Create and start sync publisher.
	p := rabbitry.NewSyncPublisher(c, rabbitry.TestLogger(t, true, 0))
	pubDone := make(chan struct{})
	go func() {
		if err := p.Start(pubCtx); err != nil {
			t.Errorf("(*rabbitry.SyncPublisher).Start(): %v", err)
		}
		pubCtxCancel()
		pubDone <- struct{}{}
	}()

	// Sending the publication without any error.
	if err := p.Publish(pubCtx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "binary/octet-stream",
		Body:        []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99},
	}); err != nil {
		t.Errorf("publishing without any error: %v", err)
	}

	// Sending the publication with magic sequence for disconnect.
	var amqpErr *amqp.Error
	if err := p.Publish(pubCtx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "binary/octet-stream",
		Body:        magicSequence,
	}); !(errors.As(err, &amqpErr) && amqpErr.Code == amqp.FrameError && amqpErr.Reason == "EOF") {
		t.Errorf("unexpected publish error: %v", err)
	}

	pubCtxCancel()
	<-pubDone
}

// TestE2E_Consumer is a test for parallel consumers.
func TestE2E_Consumer(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	queue, ch := declareTestQueue(ctx, t, c)

	messageCount := 100

	// Sending publications to queue for fill it with test data (async sending).
	for i := 0; i < messageCount; i++ {
		n := i + 1
		msgID := strconv.Itoa(n)
		if err := ch.Publish("", queue, false, false, amqp.Publishing{
			AppId:       "rabbitry_e2e",
			MessageId:   msgID,
			ContentType: "text/plain",
			Body:        []byte("content on message " + msgID),
		}); err != nil {
			t.Errorf("[n=%d] (*amqp.Channel).Publish(): %s", n, err)
		}
	}

	// Create consumer.
	consumer := rabbitry.NewConsumer(c, rabbitry.TestLogger(t, true, 0))

	cCtx, cCtxCancel := context.WithCancel(ctx)
	defer cCtxCancel()

	var (
		wg           sync.WaitGroup
		consumeCount int32
	)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if cErr := consumer.StartLoop(cCtx,
				func(c *amqp.Channel) (<-chan amqp.Delivery, error) {
					if err := c.Qos(1, 0, false); err != nil {
						return nil, fmt.Errorf("(*amqp.Channel).Qos(): %w", err)
					}
					ctag := fmt.Sprintf("consume-%d", n)
					return c.Consume(queue, ctag, false, false, false, false, nil)
				},
				func(d *amqp.Delivery) {
					if d.AppId != "rabbitry_e2e" {
						t.Errorf("[n=%d] unexpected delivery: %+v", n, d)
					}
					if err := d.Ack(false); err != nil {
						t.Errorf("[n=%d] failed to acknowledge delivery: %v", n, err)
					}

					// Stop the consumer's work if the entire delivery is consumed.
					if atomic.AddInt32(&consumeCount, 1) == int32(messageCount) {
						cCtxCancel()
					}
				},
			); cErr != nil {
				t.Errorf("[n=%d] (*rabbitry.Consumer).StartLoop(): %v", n, cErr)
				cCtxCancel()
			}
		}(i + 1)
	}

	wg.Wait()

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != 0 {
		t.Errorf("after consumer work  a queue is not empty (messages: %d)", q.Messages)
	}
}

// TestE2E_Consumer_Channel_ErrClosed is a test for consumer reconnection logic
// on closing connection channel while start delivering.
func TestE2E_Consumer_StartDelivering_ErrClosed(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	queue, ch := declareTestQueue(ctx, t, c)
	if err := ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("content on message"),
	}); err != nil {
		t.Fatalf("(*amqp.Channel).Publish(): %s", err)
	}

	// Create consumer.
	consumer := rabbitry.NewConsumer(c, rabbitry.TestLogger(t, true, 0))

	cCtx, cCtxCancel := context.WithCancel(ctx)
	defer cCtxCancel()

	var repeat bool
	if cErr := consumer.StartLoop(cCtx,
		func(c *amqp.Channel) (<-chan amqp.Delivery, error) {
			if !repeat {
				repeat = true
				if er := c.Close(); er != nil {
					t.Errorf("closing connection channel while emulate disconnect: %v", er)
				}
			}
			return c.Consume(queue, "", false, false, false, false, nil)
		},
		func(d *amqp.Delivery) {
			if err := d.Ack(false); err != nil {
				t.Errorf("failed to acknowledge delivery: %v", err)
			}

			// Stop the consumer's work if the entire delivery is consumed.
			cCtxCancel()
		},
	); cErr != nil {
		t.Errorf("(*rabbitry.Consumer).StartLoop(): %v", cErr)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != 0 {
		t.Errorf("after consumer work  a queue is not empty (messages: %d)", q.Messages)
	}
}

// TestE2E_Consumer_StartDelivering_HandlerPanic is a test for emulation
// consumer recovery after hendler panic. Messages will process several times,
// causing panic during the first five treatments for each message.
func TestE2E_Consumer_StartDelivering_HandlerPanic(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	messages := []amqp.Publishing{
		{
			MessageId:   "1",
			ContentType: "text/plain",
			Body:        []byte("content on message 1"),
		},
		{
			MessageId:   "2",
			ContentType: "text/plain",
			Body:        []byte("content on message 2"),
		},
		{
			MessageId:   "3",
			ContentType: "text/plain",
			Body:        []byte("content on message 3"),
		},
	}

	queue, ch := declareTestQueue(ctx, t, c)
	for i, msg := range messages {
		if err := ch.Publish("", queue, false, false, msg); err != nil {
			t.Fatalf("(*amqp.Channel).Publish(): message #%d: %s", i+1, err)
		}
	}

	// Create consumer.
	consumer := rabbitry.NewConsumer(c, rabbitry.TestLogger(t, false, 0))

	cCtx, cCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer cCtxCancel()

	panicMaxLimit := 5
	panicCounts := make(map[string]int, len(messages))

	var processed int
	if cErr := consumer.StartLoop(cCtx,
		func(c *amqp.Channel) (<-chan amqp.Delivery, error) {
			return c.Consume(queue, "", false, false, false, false, nil)
		},
		func(d *amqp.Delivery) {
			if panicCounts[d.MessageId] < panicMaxLimit {
				panicCounts[d.MessageId]++
				panic("test handler panic for message " + d.MessageId) // <– Panic here!
			}

			if err := d.Ack(false); err != nil {
				t.Errorf("failed to acknowledge delivery: %v", err)
			}

			processed++

			if processed >= len(messages) {
				// Stop the consumer's work if the entire delivery is consumed.
				cCtxCancel()
			}
		},
	); cErr != nil {
		t.Errorf("(*rabbitry.Consumer).StartLoop(): %v", cErr)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != 0 {
		t.Errorf("after consumer work a queue is not empty (messages: %d)", q.Messages)
	}
}

// TestE2E_Consumer_NackWithoutRequeueOnPanic is a test for emulation
// consumer recovery after hendler panic. In this test case delivery
// will be NACK without requeue.
func TestE2E_Consumer_NackWithoutRequeueOnPanic(t *testing.T) {
	t.Parallel()

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// Create client.
	c, err := rabbitry.New(ctx, connectionURL(t), &rabbitry.Config{
		Logger: rabbitry.TestLogger(t, true, 0),
	})
	if err != nil {
		t.Fatalf("rabbitry.New: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			t.Errorf("(*rabbitry.Client).Close(): %s", err)
		}
	}()

	queue, ch := declareTestQueue(ctx, t, c)
	if err := ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("content on message"),
	}); err != nil {
		t.Fatalf("(*amqp.Channel).Publish(): %s", err)
	}

	// Create consumer.
	consumer := rabbitry.NewConsumer(
		c,
		rabbitry.TestLogger(t, false, 0),
		rabbitry.ConsumerWithRequeueOnPanic(false), // <– Disable delivery requeue on panic.
	)

	cCtx, cCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer cCtxCancel()

	var calls int
	if cErr := consumer.StartLoop(cCtx,
		func(c *amqp.Channel) (<-chan amqp.Delivery, error) {
			return c.Consume(queue, "", false, false, false, false, nil)
		},
		func(d *amqp.Delivery) {
			calls++

			if calls == 1 {
				// Terminate consumer, no messages are expected.
				time.AfterFunc(500*time.Millisecond, cCtxCancel)

				panic("test handler panic for message") // <– Panic here!
			}

			t.Errorf("unexpected callback call after panic (call: %d)", calls)

			if err := d.Ack(false); err != nil {
				t.Errorf("failed to acknowledge unexpected delivery: %v", err)
			}
		},
	); cErr != nil {
		t.Errorf("(*rabbitry.Consumer).StartLoop(): %v", cErr)
	}

	q, err := ch.QueueInspect(queue)
	if err != nil {
		t.Fatalf("queue inspect: %v", err)
	}
	if q.Messages != 0 {
		t.Errorf("after consumer work a queue is not empty (messages: %d)", q.Messages)
	}
}
