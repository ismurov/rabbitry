package rabbitry

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Logger is a simple wrapper of a logging function.
//
// In reality the users might actually use different logging libraries, and they
// are not always compatible with each other.
//
// Logger is meant to be a simple common ground that it's easy to wrap whatever
// logging library they use into.
type Logger func(msg string)

// NopLogger is a Logger implementation that does nothing.
func NopLogger(msg string) {}

// StdLogger wraps stdlib log package into a Logger.
//
// If logger passed in is nil, it will fallback to use stderr and default flags.
func StdLogger(logger *log.Logger) Logger {
	if logger == nil {
		logger = log.New(os.Stderr, "[rabbitry] ", log.LstdFlags|log.Lmsgprefix)
	}
	return func(msg string) {
		logger.Output(2, msg) //nolint:errcheck // Logger for logger – it's funny.
	}
}

// TestLogger is a Logger implementation can be used in test codes. If asError is true
// it fails the test when being called. The calldepth is used to adjust the call depth
// of the logger, no correction is required for direct use.
func TestLogger(tb testing.TB, asError bool, calldepth int) Logger {
	tb.Helper()

	logger := tb.Log
	if asError {
		logger = tb.Error
	}

	return func(msg string) {
		file, line := runtimeCaller(calldepth + 1)
		file = filepath.Join(
			filepath.Base(filepath.Dir(file)),
			filepath.Base(file),
		)
		logger(
			logf("(file %s:%d) logger called with msg: %q", file, line, msg),
		)
	}
}

// logf is a shorter formatter for logging messages.
func logf(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

// runtimeCaller is a wrapped around runtime.Caller with fallback values.
func runtimeCaller(calldepth int) (file string, line int) {
	_, file, line, ok := runtime.Caller(calldepth + 1)
	if !ok {
		return "???", 0
	}
	return file, line
}
