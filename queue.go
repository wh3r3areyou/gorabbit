package gorabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// Init queue, init exchange, bind exchange + queue
func bind(channel *amqp.Channel, nameExchange, typeExchange, routingKey, nameQueue string) error {
	err := initializeExchange(channel, nameExchange, typeExchange)

	if err != nil {
		return fmt.Errorf("can`t initialize exchange: %s", err.Error())
	}

	err = initializeQueue(channel, nameQueue)

	if err != nil {
		return fmt.Errorf("can`t initialize queue: %s", err.Error())
	}

	err = channel.QueueBind(
		nameQueue,    // queue name
		routingKey,   // routing key
		nameExchange, // exchange
		false,
		nil)

	if err != nil {
		return fmt.Errorf("can`t bind queue and exchange: %s", err.Error())
	}

	return nil
}

func initializeExchange(channel *amqp.Channel, nameExchange string, typeExchange string) error {
	switch typeExchange {
	case Direct:
		break
	case Fanout:
		break
	case Topic:
		break
	case Custom:
	default:
		log.Fatal("there is no such type exchange")
	}

	return channel.ExchangeDeclare(
		nameExchange,
		typeExchange,
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
