package rabbitmq

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"time"

	"github.com/Deivisson/go-rabbitmq/types"
	"github.com/Deivisson/go-rabbitmq/util"
	"github.com/streadway/amqp"
)

type PublisherConfig struct {
	types.BaseConfig
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

// Delivery receive as parameter a msg interface or amqp.Publishing instance and will
// publish on Rabbitmq server
func (p *Publisher) Delivery(msg interface{}, publishing *amqp.Publishing) error {
	if msg == nil && publishing == nil {
		return errors.New("msg or publishing parameters are required")
	}

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

	config := &p.Config
	d := util.Declarant{Config: &p.Config.BaseConfig, Channel: channel}
	if err := d.PrepareQueuesExchange(); err != nil {
		return err
	}

	if config.BaseConfig.Queue == nil {
		config.BaseConfig.Queue = &types.Queue{}
	}

	if publishing != nil {
		if err := p.send(channel, publishing); err != nil {
			return err
		}
	}

	messages := getMessages(msg)
	for i := 0; i < len(messages); i++ {
		pm, err := buildPublishing(messages[i])
		if err != nil {
			return err
		}

		if err := p.send(channel, pm); err != nil {
			return err
		}
	}
	return nil
}

// send publish the message to channel
func (p *Publisher) send(chn *amqp.Channel, publishing *amqp.Publishing) error {
	config := p.Config
	return chn.Publish(
		config.BaseConfig.Exchange.Name,
		config.BaseConfig.Queue.RoutingKey,
		config.Mandatory,
		config.Immediate,
		*publishing,
	)
}

// getMessages parse the inputed message and normalize it, always returning array of array of bytes
func getMessages(data interface{}) [][]byte {
	var messages [][]byte

	switch data.(type) {
	case []interface{}:
		v := reflect.ValueOf(data)
		for i := 0; i < v.Len(); i++ {
			item := v.Index(i).Interface().(*map[string]interface{})
			j, _ := json.Marshal(item)
			messages = append(messages, j)
		}
	case []byte:
		v := reflect.ValueOf(data)
		messages = append(messages, v.Bytes())
	case string:
		v := reflect.ValueOf(data)
		s := v.Interface().(string)
		messages = append(messages, []byte(s))
	}
	return messages
}

func buildPublishing(msg []byte) (*amqp.Publishing, error) {
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
		Body:            msg,
	}, nil
}
