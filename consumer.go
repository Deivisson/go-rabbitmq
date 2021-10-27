package rabbitmq

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type Exchange struct {
	Name    string
	Type    string
	Declare bool
}

type Queue struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp.Table
	RoutingKey    string
	BindExchanges *[]string
}

type RetryConfig struct {
	RetryQueue    *Queue
	RetryExchange *Exchange
	RetryTimeout  int
	ErrorQueue    *Queue
	ErrorExchange *Exchange
}

type ConsumerConfig struct {
	Exchange      *Exchange
	Queue         *Queue
	Retryable     bool
	RetryConfig   *RetryConfig
	ConsumerName  string
	ConsumerCount int
	PrefetchCount int
	Reconnect     struct {
		MaxAttempt int
		Interval   time.Duration
	}
}

type Consumer struct {
	Config ConsumerConfig
	Rabbit *Rabbit
}

// NewConsumer returns a consumer instance.
func NewConsumer(config ConsumerConfig, rabbit *Rabbit) *Consumer {
	return &Consumer{
		Config: config,
		Rabbit: rabbit,
	}
}

// Start declares all the necessary components of the consumer and
// runs the consumers. This is called one at the application start up
// or when consumer needs to reconnects to the server.
func (c *Consumer) Start(worker Worker) error {
	con, err := c.Rabbit.Connection()
	if err != nil {
		return err
	}
	go c.closedConnectionListener(con.NotifyClose(make(chan *amqp.Error)), worker)

	channel, err := con.Channel()
	if err != nil {
		return err
	}

	if err = c.declareRetryQueues(channel); err != nil {
		return err
	}

	if err = c.declareDefaults(channel); err != nil {
		return err
	}

	if err := channel.Qos(c.Config.PrefetchCount, 0, false); err != nil {
		return err
	}

	for i := 1; i <= c.Config.ConsumerCount; i++ {
		id := i
		go worker.Consume(channel, id)
	}

	// Simulate manual connection close
	//_ = con.Close()

	return nil
}

func (c *Consumer) declareDefaults(chn *amqp.Channel) error {
	if err := declareExchange(chn, c.Config.Exchange); err != nil {
		return err
	}

	if c.Config.Retryable && c.Config.Queue.Args == nil {
		c.Config.Queue.Args = map[string]interface{}{
			"x-dead-letter-exchange": fmt.Sprintf("%s-retry", c.Config.Queue.Name),
		}
	}

	if err := declareQueue(chn, c.Config.Queue); err != nil {
		return err
	}

	if err := bindQueue(chn, c.Config.Queue, c.Config.Exchange); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) declareRetryQueues(chn *amqp.Channel) error {
	if !c.Config.Retryable {
		return nil
	}

	rc := c.Config.RetryConfig
	if rc == nil {
		rc = &RetryConfig{}
	}

	prepareRetryParams(rc, c.Config.Queue.Name)

	if err := declareExchange(chn, rc.RetryExchange); err != nil {
		return err
	}

	if err := declareQueue(chn, rc.RetryQueue); err != nil {
		return err
	}

	if err := bindQueue(chn, rc.RetryQueue, rc.RetryExchange); err != nil {
		return err
	}

	if err := declareExchange(chn, rc.ErrorExchange); err != nil {
		return err
	}

	if err := declareQueue(chn, rc.ErrorQueue); err != nil {
		return err
	}

	if err := bindQueue(chn, rc.ErrorQueue, rc.ErrorExchange); err != nil {
		return err
	}

	return nil
}

func prepareRetryParams(rf *RetryConfig, defaultQueueName string) {
	if rf.RetryQueue == nil {
		rf.RetryQueue = &Queue{
			Name:    fmt.Sprintf("%s-retry", defaultQueueName),
			Durable: true,
			Args: map[string]interface{}{
				"x-dead-letter-exchange": defaultQueueName,
				"x-message-ttl":          60 * 1000,
			},
		}
	}

	if rf.ErrorQueue == nil {
		rf.ErrorQueue = &Queue{
			Name:    fmt.Sprintf("%s-error", defaultQueueName),
			Durable: true,
		}
	}

	if rf.RetryExchange == nil {
		rf.RetryExchange = &Exchange{
			Name:    rf.RetryQueue.Name,
			Type:    amqp.ExchangeDirect,
			Declare: true,
		}
	}

	if rf.ErrorExchange == nil {
		rf.ErrorExchange = &Exchange{
			Name:    rf.ErrorQueue.Name,
			Type:    amqp.ExchangeDirect,
			Declare: true,
		}
	}
}

func declareExchange(chn *amqp.Channel, ex *Exchange) error {
	if ex == nil || !ex.Declare {
		return nil
	}
	return chn.ExchangeDeclare(
		ex.Name,
		ex.Type,
		true,
		false,
		false,
		false,
		nil,
	)
}

func declareQueue(chn *amqp.Channel, que *Queue) error {
	_, err := chn.QueueDeclare(
		que.Name,
		que.Durable,
		que.AutoDelete,
		que.Exclusive,
		que.NoWait,
		que.Args,
	)
	return err
}

func bindQueue(chn *amqp.Channel, que *Queue, ex *Exchange) error {
	if ex != nil {
		if err := chn.QueueBind(
			que.Name,
			que.RoutingKey,
			ex.Name,
			false,
			nil,
		); err != nil {
			return err
		}
	}

	if que.BindExchanges != nil {
		for _, v := range *que.BindExchanges {
			if err := chn.QueueBind(
				que.Name,
				que.RoutingKey,
				v,
				false,
				nil,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// closedConnectionListener attempts to reconnect to the server and
// reopens the channel for set amount of time if the connection is
// closed unexpectedly. The attempts are spaced at equal intervals.
func (c *Consumer) closedConnectionListener(closed <-chan *amqp.Error, worker Worker) {
	log.Println("INFO: Watching closed connection")

	// If you do not want to reconnect in the case of manual disconnection
	// via RabbitMQ UI or Server restart, handle `amqp.ConnectionForced`
	// error code.
	err := <-closed
	if err != nil {
		log.Println("INFO: Closed connection:", err.Error())

		var i int

		for i = 0; i < c.Config.Reconnect.MaxAttempt; i++ {
			log.Println("INFO: Attempting to reconnect")

			if err := c.Rabbit.Connect(); err == nil {
				log.Println("INFO: Reconnected")

				if err := c.Start(worker); err == nil {
					break
				}
			}

			time.Sleep(c.Config.Reconnect.Interval)
		}

		if i == c.Config.Reconnect.MaxAttempt {
			log.Println("CRITICAL: Giving up reconnecting")

			return
		}
	} else {
		log.Println("INFO: Connection closed normally, will not reconnect")
		os.Exit(0)
	}
}
