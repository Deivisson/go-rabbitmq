package util

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Deivisson/go-rabbitmq/types"
	"github.com/streadway/amqp"
)

type Declarant struct {
	Config  *types.BaseConfig
	Channel *amqp.Channel
}

const (
	DEFAULT_RETRY_TIMEOUT      = 60 * 1000 // 60 seconds
	DEFAULT_RETRY_COUNT        = 5
	DEFAULT_RETRY_QUEUE_SUFFIX = "retry"
	DEFAULT_ERROR_QUEUE_SUFFIX = "error"
)

func (d *Declarant) PrepareQueuesExchange() error {
	if err := d.declareRetryQueues(); err != nil {
		return err
	}
	if err := d.DeclareDefaults(); err != nil {
		return err
	}
	return nil
}

func (d *Declarant) DeclareDefaults() error {
	if err := declareExchange(d.Channel, d.Config.Exchange); err != nil {
		return err
	}

	if d.Config.Retryable && d.Config.Queue.Args == nil {
		d.Config.Queue.Args = map[string]interface{}{
			"x-dead-letter-exchange": fmt.Sprintf("%s-%s", d.Config.Queue.Name, DEFAULT_RETRY_QUEUE_SUFFIX),
		}
	}

	if err := declareQueue(d.Channel, d.Config.Queue); err != nil {
		return err
	}

	if err := bindQueue(d.Channel, d.Config.Queue, d.Config.Exchange); err != nil {
		return err
	}

	return nil
}

func (d *Declarant) declareRetryQueues() error {
	if !d.Config.Retryable {
		return nil
	}

	if d.Config.RetryConfig == nil {
		d.Config.RetryConfig = &types.RetryConfig{}
		d.prepareRetryParams(d.Config.RetryConfig)
	}
	rc := d.Config.RetryConfig

	if err := declareExchange(d.Channel, rc.RetryExchange); err != nil {
		return err
	}

	if err := declareQueue(d.Channel, rc.RetryQueue); err != nil {
		return err
	}

	if err := bindQueue(d.Channel, rc.RetryQueue, rc.RetryExchange); err != nil {
		return err
	}

	if err := declareExchange(d.Channel, rc.ErrorExchange); err != nil {
		return err
	}

	if err := declareQueue(d.Channel, rc.ErrorQueue); err != nil {
		return err
	}

	if err := bindQueue(d.Channel, rc.ErrorQueue, rc.ErrorExchange); err != nil {
		return err
	}

	return nil
}

func (d *Declarant) prepareRetryParams(rf *types.RetryConfig) {
	defaultQueueName := d.Config.Queue.Name
	exchange := d.Config.Exchange
	dle := defaultQueueName
	if exchange != nil {
		dle = exchange.Name
	}

	if rf.RetryQueue == nil {
		timeout := rf.RetryTimeout
		if timeout == 0 {
			timeout = DEFAULT_RETRY_TIMEOUT
		}
		rf.RetryQueue = &types.Queue{
			Name:       fmt.Sprintf("%s-%s", defaultQueueName, DEFAULT_RETRY_QUEUE_SUFFIX),
			Durable:    true,
			RoutingKey: "#",
			Args: map[string]interface{}{
				"x-dead-letter-exchange":    dle,
				"x-message-ttl":             timeout,
				"x-dead-letter-routing-key": defaultQueueName,
			},
		}
	}

	if rf.ErrorQueue == nil {
		rf.ErrorQueue = &types.Queue{
			Name:       fmt.Sprintf("%s-%s", defaultQueueName, DEFAULT_ERROR_QUEUE_SUFFIX),
			Durable:    true,
			RoutingKey: "#",
		}
	}

	if rf.RetryExchange == nil {
		rf.RetryExchange = &types.Exchange{
			Name:    rf.RetryQueue.Name,
			Type:    amqp.ExchangeTopic,
			Durable: true,
			Declare: true,
		}
	}

	if rf.ErrorExchange == nil {
		rf.ErrorExchange = &types.Exchange{
			Name:    rf.ErrorQueue.Name,
			Type:    amqp.ExchangeTopic,
			Durable: true,
			Declare: true,
		}
	}

	if rf.RetryCount == 0 {
		rf.RetryCount = DEFAULT_RETRY_COUNT
	}
}

func declareExchange(chn *amqp.Channel, ex *types.Exchange) error {
	if ex == nil || !ex.Declare {
		return nil
	}

	if strings.TrimSpace(ex.Name) == "" {
		return errors.New("exchange name are required")
	}

	if strings.TrimSpace(ex.Type) == "" {
		ex.Type = amqp.ExchangeDirect
	}

	return chn.ExchangeDeclare(
		ex.Name,
		ex.Type,
		ex.Durable,
		ex.AutoDelete,
		ex.Internal,
		ex.NoWait,
		ex.Args,
	)
}

func declareQueue(chn *amqp.Channel, que *types.Queue) error {
	if que == nil || !que.Declare {
		return nil
	}

	if strings.TrimSpace(que.Name) == "" {
		return errors.New("queue name are required")
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

func bindQueue(chn *amqp.Channel, que *types.Queue, ex *types.Exchange) error {
	if que == nil {
		return nil
	}

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
