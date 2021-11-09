package rabbitmq

import (
	"errors"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type PublisherConfig struct {
	Exchange  *Exchange
	Queue     *Queue
	Mandatory bool
	Immediate bool
}

type Publisher struct {
	Config PublisherConfig
	Rabbit *Rabbit
}

var rabbitConfig = RabbitConfig{
	Schema:   "amqp",
	Username: "guest",
	Password: "guest",
	Host:     os.Getenv("AMQP_HOST"),
	Port:     "5672",
	VHost:    "/",
}

// NewPublisher returns a publisher instance.
func NewPublisher(config PublisherConfig) *Publisher {
	return &Publisher{
		Config: config,
		Rabbit: NewRabbit(rabbitConfig),
	}
}

func (p *Publisher) Delivery(msg *string, publishing *amqp.Publishing) error {
	if err := p.Rabbit.Connect(); err != nil {
		return err
	}

	con, err := p.Rabbit.Connection()
	if err != nil {
		return err
	}
	defer con.Close()

	channel, err := con.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	config := p.Config
	prepareConfigs(channel, &config)

	pm, err := getPublishing(msg, publishing)
	if err != nil {
		return err
	}
	return channel.Publish(
		config.Exchange.Name,
		config.Queue.RoutingKey,
		config.Mandatory,
		config.Immediate,
		*pm,
	)
}

func prepareConfigs(chn *amqp.Channel, config *PublisherConfig) error {
	// Exchange
	if config.Exchange == nil {
		config.Exchange = &Exchange{Name: ""}
	}

	if err := declareExchange(chn, config.Exchange); err != nil {
		return err
	}

	// Queue
	if config.Queue == nil {
		config.Queue = &Queue{Name: "", RoutingKey: ""}
	}

	if err := declareQueue(chn, config.Queue); err != nil {
		return err
	}

	if err := bindQueue(chn, config.Queue, config.Exchange); err != nil {
		return err
	}

	return nil
}

func getPublishing(msg *string, publishing *amqp.Publishing) (*amqp.Publishing, error) {
	if msg == nil && publishing == nil {
		return nil, errors.New("msg or publishing parameters are required")
	}
	if msg == nil {
		return publishing, nil
	}
	return &amqp.Publishing{
		Headers:         map[string]interface{}{},
		ContentType:     "text/plain",
		ContentEncoding: "",
		DeliveryMode:    0,
		Priority:        0,
		CorrelationId:   "",
		ReplyTo:         "",
		Expiration:      "",
		MessageId:       "",
		Timestamp:       time.Time{},
		Type:            "",
		UserId:          "",
		AppId:           "",
		Body:            []byte(*msg),
	}, nil
}
