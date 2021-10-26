package rabbitmq

import (
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type ConsumerConfig struct {
	ExchangeName    string
	ExchangeType    string
	DeclareExchange bool
	RoutingKey      string
	QueueName       string
	ConsumerName    string
	ConsumerCount   int
	PrefetchCount   int
	Reconnect       struct {
		MaxAttempt int
		Interval   time.Duration
	}
	BindExchanges *[]string
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

	if err = c.declareExchange(channel); err != nil {
		return err
	}

	if err = c.declareQueue(channel); err != nil {
		return err
	}

	if err = c.bindQueue(channel); err != nil {
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

func (c *Consumer) declareExchange(chn *amqp.Channel) error {
	config := c.Config
	if !config.DeclareExchange {
		return nil
	}
	return chn.ExchangeDeclare(
		c.Config.ExchangeName,
		c.Config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
}

func (c *Consumer) declareQueue(chn *amqp.Channel) error {
	_, err := chn.QueueDeclare(
		c.Config.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	return err
}

func (c *Consumer) bindQueue(chn *amqp.Channel) error {
	var config = c.Config
	if config.ExchangeName != "" {
		return chn.QueueBind(
			config.QueueName,
			config.RoutingKey,
			config.ExchangeName,
			false,
			nil,
		)
	}

	for _, v := range *config.BindExchanges {
		if err := chn.QueueBind(
			config.QueueName,
			config.RoutingKey,
			v,
			false,
			nil,
		); err != nil {
			return err
		}
	}
	return nil
}
