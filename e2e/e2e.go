// Package e2e provides tools for integration testing for rabbitry package.
package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/streadway/amqp"
)

const (
	// rabbitmqVhost is the name of default RabbitMQ vhost.
	rabbitmqVhost = "test-e2e"

	// rabbitmqUser and rabbitmqPassword are the username and password for
	// connecting to the RabbitMQ. These values are only used for testing.
	rabbitmqUser     = "test-user"
	rabbitmqPassword = "testing123"

	// defaultRabbitMQImageRef is the default RabbitMQ container to use if none is
	// specified.
	defaultRabbitMQImageRef = "rabbitmq:3-management-alpine"

	// rabbitmqImageManagementTagSign is a sign of management plugin in RabbitMQ container tag.
	rabbitmqImageManagementTagSign = "management"
)

// TestInstance is a wrapper around the Docker-based RabbitMQ instance.
type TestInstance struct {
	outsideContainer bool
	pool             *dockertest.Pool
	container        *dockertest.Resource
	apiHost          string
	url              *url.URL

	skipReason string
}

// MustTestInstance is NewTestInstance, except it prints errors to stderr and
// calls os.Exit when finished. Callers can call Close or MustClose().
func MustTestInstance() *TestInstance {
	testInstance, err := NewTestInstance()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	return testInstance
}

// NewTestInstance creates a new Docker-based RabbitMQ instance.
//
// This should not be used outside of testing, but it is exposed in the package
// so it can be shared with other packages. It should be called and instantiated
// in TestMain.
//
// All RabbitMQ tests can be skipped by running `go test -short` or by setting
// the `RABBITRY_E2E_SKIP_TESTS` environment variable.
//
// The RabbitMQ container image can be overridden by setting the `CI_RABBITMQ_IMAGE`
// environment variable. The image must be a RabbitMQ container.
//
// If for testing used outside container (e.g. GitLab CI) or self-hosted instance
// of RabbitMQ use the `RABBITRY_E2E_RABBITMQ_URL` environment variable for direct
// connection an skipping Docker-based RabbitMQ initialisation.
func NewTestInstance() (*TestInstance, error) {
	// Querying for -short requires flags to be parsed.
	if !flag.Parsed() {
		flag.Parse()
	}

	// Do not create an instance in -short mode.
	if testing.Short() {
		return &TestInstance{
			skipReason: "🚧 Skipping rabbitmq tests (-short flag provided)!",
		}, nil
	}

	// Do not create an instance if rabbitmq tests are explicitly skipped.
	if skip, _ := strconv.ParseBool(os.Getenv("RABBITRY_E2E_SKIP_TESTS")); skip {
		return &TestInstance{
			skipReason: "🚧 Skipping rabbitmq tests (RABBITRY_E2E_SKIP_TESTS is set)!",
		}, nil
	}

	if uri := os.Getenv("RABBITRY_E2E_RABBITMQ_URL"); uri != "" {
		return newTestInstanceURI(uri)
	}
	return newTestInstanceDocker()
}

// newTestInstanceDocker creates a new URI-based RabbitMQ instance.
func newTestInstanceURI(uri string) (*TestInstance, error) {
	// Parse the given AMQP URI according to the specification.
	u, err := amqp.ParseURI(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse provided to rabbitmq server connection url: %w", err)
	}

	// Build the connection URL.
	connectionURL := &url.URL{
		Scheme: u.Scheme,
		User:   url.UserPassword(u.Username, u.Password),
		Host:   net.JoinHostPort(u.Host, strconv.Itoa(u.Port)),
		Path:   u.Vhost,
	}

	// Try to establish a test connection to RabbitMQ server.
	conn, err := amqp.Dial(connectionURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to establish test connection to rabbitmq server (url: %s): %w", connectionURL.Redacted(), err)
	}
	if err := conn.Close(); err != nil {
		return nil, fmt.Errorf("failed to close test connection: %w", err)
	}

	// Creaate the instance.
	i := &TestInstance{
		outsideContainer: true,
		apiHost:          net.JoinHostPort(u.Host, "15672"), // Used hardcoded port RabbitMQ Management HTTP API.
		url:              connectionURL,
	}

	// Check access and availability of Management HTTP API.
	// Try to get a list of all vhosts.
	if err := i.apiDo(http.MethodGet, "/api/vhosts"); err != nil {
		return nil, fmt.Errorf("failed to check access to rabbitmq management http api: %w", err)
	}

	// Return the instance.
	return i, nil
}

// newTestInstanceDocker creates a new Docker-based RabbitMQ instance.
// The RabbitMQ container image can be overridden by setting the "CI_RABBITMQ_IMAGE"
// environment variable. The image must be a RabbitMQ container.
func newTestInstanceDocker() (*TestInstance, error) {
	// Create the pool.
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("failed to create rabbitmq docker pool: %w", err)
	}

	// Determine the container image to use.
	repository, tag, err := rabbitmqRepo(
		os.Getenv("RABBITRY_E2E_RABBITMQ_IMAGE"),
		defaultRabbitMQImageRef,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to determine rabbitmq repository: %w", err)
	}

	// Start the actual container.
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: repository,
		Tag:        tag,
		Env: []string{
			"RABBITMQ_DEFAULT_VHOST=" + rabbitmqVhost,
			"RABBITMQ_DEFAULT_USER=" + rabbitmqUser,
			"RABBITMQ_DEFAULT_PASS=" + rabbitmqPassword,
		},
	}, func(c *docker.HostConfig) {
		c.AutoRemove = true
		c.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start rabbitmq container: %w", err)
	}

	// Stop the container after its been running for too long. No since test suite
	// should take super long.
	if err := container.Expire(120); err != nil {
		return nil, fmt.Errorf("failed to expire rabbitmq container: %w", err)
	}

	// Build the connection URL.
	connectionURL := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(rabbitmqUser, rabbitmqPassword),
		Host:   container.GetHostPort("5672/tcp"),
		Path:   rabbitmqVhost,
	}

	// Try to establish a connection to RabbitMQ, with retries.
	if err := pool.Retry(func() error {
		if _, er := amqp.Dial(connectionURL.String()); er != nil {
			return er
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed waiting for rabbitmq container to be ready: %w", err)
	}

	// Return the instance.
	return &TestInstance{
		pool:      pool,
		container: container,
		apiHost:   container.GetHostPort("15672/tcp"),
		url:       connectionURL,
	}, nil
}

// MustClose is like Close except it prints the error to stderr and calls os.Exit.
func (i *TestInstance) MustClose() {
	if err := i.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// Close terminates the test RabbitMQ instance, cleaning up any resources.
func (i *TestInstance) Close() error {
	// Do not attempt to close things when there's nothing to close.
	if i.skipReason != "" {
		return nil
	}

	if !i.outsideContainer {
		if err := i.pool.Purge(i.container); err != nil {
			return fmt.Errorf("failed to purge rabbitmq container: %w", err)
		}
	}

	return nil
}

// NewVhost creates a new vhost suitable for use in testing. It returns
// an connection url to that vhost.
func (i *TestInstance) NewVhost(tb testing.TB) *url.URL {
	tb.Helper()

	// Ensure we should actually create the RabbitMQ instance.
	if i.skipReason != "" {
		tb.Skip(i.skipReason)
	}

	// Create new independent vhost.
	vhost, err := i.createVhost()
	if err != nil {
		tb.Fatal(err)
	}

	// Build the new connection URL for the new vhost name.
	connectionURL := i.url.ResolveReference(&url.URL{Path: vhost})

	// Delete vhost when done.
	tb.Cleanup(func() {
		if err := i.deleteVhost(vhost); err != nil {
			tb.Error(err)
		}
	})

	return connectionURL
}

// createVhost creates a new vhost with a random name.
func (i *TestInstance) createVhost() (string, error) {
	// Generate a random vhost name.
	name, err := randomVhostName(strings.TrimLeft(i.url.Path, "/"))
	if err != nil {
		return "", fmt.Errorf("failed to generate random vhost name: %w", err)
	}

	// Create vhost with api call.
	if err := i.apiDo(http.MethodPut, "/api/vhosts/"+name); err != nil {
		return "", fmt.Errorf("failed to create vhost: %w", err)
	}
	return name, nil
}

// deleteVhost deletes a vhost.
func (i *TestInstance) deleteVhost(name string) error {
	// Delete vhost with api call.
	if err := i.apiDo(http.MethodDelete, "/api/vhosts/"+name); err != nil {
		return fmt.Errorf("failed to delete vhost: %w", err)
	}
	return nil
}

// apiDo executes call to RabbitMQ Management HTTP API.
func (i *TestInstance) apiDo(method, pth string) error {
	endpoint := fmt.Sprintf("http://%s%s", i.apiHost, pth)
	req, err := http.NewRequest(method, endpoint, http.NoBody)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	pass, _ := i.url.User.Password()
	req.SetBasicAuth(i.url.User.Username(), pass)
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	errPrefix := func() string {
		return fmt.Sprintf("%s %s - %d", strings.ToUpper(req.Method), req.URL.String(), resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s: failed to read body: %w", errPrefix(), err)
	}

	code := resp.StatusCode
	if code < 200 || code >= 300 {
		return fmt.Errorf("%s: expected 2xx response; %s", errPrefix(), body)
	}

	return nil
}

// postgresRepo checks the container reference and splits it into a repository and a tag.
// If reference is not defined, a fallback will be returned.
func rabbitmqRepo(ref, fallback string) (string, string, error) {
	if ref == "" {
		ref = fallback
	}
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid reference for rabbitmq container: %q", ref)
	}

	if !strings.Contains(parts[1], rabbitmqImageManagementTagSign) {
		return "", "", fmt.Errorf("rabbitmq container doesn't provided with the management plugin installed: %q", ref)
	}

	return parts[0], parts[1], nil
}

// randomVhostName returns a random vhost name. If the prefix is empty,
// uses rabbitmqVhost as default vhost prefix.
func randomVhostName(prefix string) (string, error) {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	name := hex.EncodeToString(b)

	if prefix == "" {
		prefix = rabbitmqVhost
	}
	return fmt.Sprintf("%s-%s", prefix, name), nil
}
