package rabbitry

import (
	"fmt"
	"time"
)

const (
	defaultHeartbeat           = 10 * time.Second
	defaultConnectionTimeout   = 30 * time.Second
	defaultReconnectionTimeout = 5 * time.Second
)

// Config is a configuration for Client and ConnectionFactory.
//
// Note: All fields are optional.
type Config struct {
	Logger Logger

	// Connection factory configuration.
	Heartbeat           time.Duration
	ConnectionTimeout   time.Duration
	ReconnectionTimeout time.Duration
}

// Validate validates configuration values.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidConfig)
	}
	if c.Heartbeat < 0 {
		return fmt.Errorf("%w: heartbeat interval must be positive", ErrInvalidConfig)
	}
	if c.ConnectionTimeout < 0 {
		return fmt.Errorf("%w: connection timeout must be positive", ErrInvalidConfig)
	}
	if c.ReconnectionTimeout < 0 {
		return fmt.Errorf("%w: reconnection timeout must be positive", ErrInvalidConfig)
	}
	return nil
}

func defaultConfig() *Config {
	return &Config{
		Logger:              StdLogger(nil),
		Heartbeat:           defaultHeartbeat,
		ConnectionTimeout:   defaultConnectionTimeout,
		ReconnectionTimeout: defaultReconnectionTimeout,
	}
}

func (c *Config) fallbackDefaults() {
	if c.Logger == nil {
		c.Logger = StdLogger(nil)
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = defaultHeartbeat
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = defaultConnectionTimeout
	}
	if c.ReconnectionTimeout == 0 {
		c.ReconnectionTimeout = defaultReconnectionTimeout
	}
}
