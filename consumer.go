package rabbitmq

import (
	"log"
	"os"
	"time"

	"github.com/Deivisson/go-rabbitmq/types"
	util "github.com/Deivisson/go-rabbitmq/util"
	"github.com/streadway/amqp"
)

type ConsumerConfig struct {
	types.BaseConfig
	ConsumerName  string
	ConsumerCount int
	PrefetchCount int
	Reconnect     types.Reconnect
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

	d := util.Declarant{Config: &c.Config.BaseConfig, Channel: channel}
	if err := d.PrepareQueuesExchange(); err != nil {
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

// Reject will realize the necessaries treatments on reject message. If Retryable enabled will manager
// the number of times that the message try to be processed entering in resilience flux.
// When RetryCount is reached, the message will sent to -error queue and removed from processing queue
func (c *Consumer) Reject(channel *amqp.Channel, msg *amqp.Delivery, requeue bool) error {
	if !c.Config.Retryable {
		return msg.Reject(requeue)
	}

	count, _ := getRetryCount(msg, c.Config.Queue.Name)
	if count >= c.Config.RetryConfig.RetryCount-1 {
		if err := channel.Publish(
			c.Config.RetryConfig.ErrorExchange.Name,
			"",
			false,
			false,
			amqp.Publishing{
				Body: msg.Body,
			},
		); err != nil {
			return err
		}
		return msg.Ack(false)
	} else {
		return msg.Reject(requeue)
	}
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

// getRetryCount get the number of times a message was on processing queue
func getRetryCount(msg *amqp.Delivery, queueName string) (int, error) {
	if v, ok := msg.Headers["x-death"]; ok {
		if v2, ok := v.([]interface{}); ok {
			for _, s := range v2 {
				x := s.(amqp.Table)
				if queueName == x["queue"] {
					return x["count"].(int), nil
				}
			}
		}
	}
	return 0, nil
}
