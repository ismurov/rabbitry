package rabbitry

import (
	"context"
	"net"
	"testing"
	"time"
)

// Test_contextDialer_contextCanceled is a test for explicit context.Canceled
// error when dial canceled by context.
func Test_contextDialer_contextCanceled(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer func() {
		if er := l.Close(); er != nil {
			t.Errorf("listener.Close()")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = contextDialer(ctx, 5*time.Second)(
		l.Addr().Network(),
		l.Addr().String(),
	)
	if err != context.Canceled { //nolint:errorlint // This is a test for explicit error checking.
		t.Errorf("contextDialer didn't handle the context cancellation error; error=%s", err)
	}
}
