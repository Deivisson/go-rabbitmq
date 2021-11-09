package rabbitmq

import (
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
	BindExchanges *[]Exchange
	Declare       bool
}

type Reconnect struct {
	MaxAttempt int
	Interval   time.Duration
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
	if que == nil || !que.Declare {
		return nil
	}
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
			routingKey := que.RoutingKey
			if v.Type == amqp.ExchangeFanout {
				routingKey = ""
			}
			if err := chn.QueueBind(
				que.Name,
				routingKey,
				v.Name,
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
