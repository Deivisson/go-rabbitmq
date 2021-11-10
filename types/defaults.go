package types

import (
	"time"

	"github.com/streadway/amqp"
)

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
	Declare    bool
}

type Queue struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp.Table
	RoutingKey    string
	BindExchanges *[]Exchange
	Declare       bool
}

type RetryConfig struct {
	RetryQueue    *Queue
	RetryExchange *Exchange
	RetryTimeout  int64
	RetryCount    int
	ErrorQueue    *Queue
	ErrorExchange *Exchange
}

type BaseConfig struct {
	Exchange    *Exchange
	Queue       *Queue
	Retryable   bool
	RetryConfig *RetryConfig
}

type Reconnect struct {
	MaxAttempt int
	Interval   time.Duration
}
