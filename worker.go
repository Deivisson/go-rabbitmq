package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Worker interface {
	// Function to be implemented by all Workers. Will call from polymorphism mecanism
	Consume(channel *amqp.Channel, id int)
}
