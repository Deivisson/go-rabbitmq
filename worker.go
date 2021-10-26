package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Worker interface {
	Consume(channel *amqp.Channel, id int)
}
