package aws_s3

import (
	"context"
	"time"
)

type config struct {
	timeout time.Duration
	useSSL  bool
}

func (c *config) NewContext() (context.Context, context.CancelFunc) {
	if c.timeout > 0 {
		return context.WithTimeout(context.Background(), c.timeout)
	}

	return context.Background(), func(){}
}

type option func(*config) ()

func WithTimeOut(timeout time.Duration) option {
	return func(config *config) {
		config.timeout = timeout
	}
}

func WithSSL() option {
	return func(config *config) {
		config.useSSL = true
	}
}
