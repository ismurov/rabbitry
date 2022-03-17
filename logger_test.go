package rabbitry

import (
	"bytes"
	"log"
	"path/filepath"
	"testing"
)

func TestStdLogger(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := StdLogger(
		log.New(&buf, "[rabbitry] ", log.Lmsgprefix|log.Lshortfile),
	)

	// WARNING: The following lines are test sensitive.
	logger("test logger message")
	file, line := runtimeCaller(0)
	line--

	// Output example:
	//	logger_test.go:30: [rabbitry] test logger message
	want := logf("%s:%d: [rabbitry] test logger message\n", filepath.Base(file), line)

	if got := buf.String(); got != want {
		t.Errorf("incorrect logger message:\ngot:  %q\nwant: %q", got, want)
	}
}
