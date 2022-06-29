package gorabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Init queue, init exchange, bind exchange + queue
func bind(channel *amqp.Channel, nameExchange string, nameQueue string) error {
	err := initializeExchange(channel, nameExchange)

	if err != nil {
		return fmt.Errorf("can`t initialize exchange: %s", err.Error())
	}

	err = initializeQueue(channel, nameQueue)

	if err != nil {
		return fmt.Errorf("can`t initialize queue: %s", err.Error())
	}

	err = channel.QueueBind(
		nameQueue,    // queue name
		"",           // routing key
		nameExchange, // exchange
		false,
		nil)

	if err != nil {
		return fmt.Errorf("can`t bind queue and exchange: %s", err.Error())
	}

	return nil
}

func initializeExchange(channel *amqp.Channel, nameExchange string) error {
	return channel.ExchangeDeclare(
		nameExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
}

func initializeQueue(channel *amqp.Channel, nameQueue string) error {
	_, err := channel.QueueDeclare(
		nameQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	return err
}
