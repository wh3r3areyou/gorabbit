package gorabbit

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type (
	Consumer struct {
		sync          bool
		tag           string
		queueName     string
		exchangeName  string
		exchangeType  string
		routingKey    string
		connectionStr string
		done          chan struct{}
		ErrorCh       chan error
		execFn        execFn
		conn          *amqp.Connection
		channel       *amqp.Channel
	}

	execFn func(message []byte)
)

// NewConsumer Initialize new consumer
func NewConsumer(config Config, execFn execFn) (*Consumer, error) {
	c := Consumer{
		tag:           config.Tag,
		queueName:     config.QueueName,
		exchangeName:  config.ExchangeName,
		connectionStr: config.Connection,
		exchangeType:  config.TypeExchange,
		routingKey:    config.RoutingKey,
		execFn:        execFn,
		done:          make(chan struct{}),
		ErrorCh:       make(chan error),
		sync:          config.Sync,
	}

	var err error
	// create connection with rabbitmq
	err = c.createConnection()
	if err != nil {
		return nil, err
	}

	// get channel
	err = c.openChannel()
	if err != nil {
		return nil, err
	}

	// bind exchange consumer and bind
	err = c.bind()
	if err != nil {
		return nil, err
	}

	go c.handleClosedConnAndChannel()

	return &c, nil
}

func (c *Consumer) handle(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		if c.sync {
			c.execFn(d.Body)
			err := d.Ack(false)
			if err != nil {
				if err != ErrClosed {
					c.ErrorCh <- err
				}
			}
		} else {
			d := d
			go func() {
				c.execFn(d.Body)
				err := d.Ack(false)
				if err != nil {
					if err != ErrClosed {
						c.ErrorCh <- err
					}
				}
			}()
		}
	}
}

// create connection with rabbitmq
func (c *Consumer) createConnection() error {
	var err error
	c.conn, err = amqp.Dial(c.connectionStr)
	if err != nil {
		return fmt.Errorf("error with create connection with rabbitmq: %s", err.Error())
	}
	return nil
}

// open channel in rabbitmq
func (c *Consumer) openChannel() error {
	var err error

	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("error with openChannel with rabbitmq: %s", err.Error())
	}

	// if we want take 1 element in queue
	if c.sync {
		if err = c.channel.Qos(1, 0, false); err != nil {
			return fmt.Errorf("error in ch.Qos: %s", err.Error())
		}
	}

	return nil
}

// bind exchange consumer and queue
func (c *Consumer) bind() error {
	err := bind(c.channel, c.exchangeName, c.exchangeType, c.routingKey, c.queueName)
	if err != nil {
		return fmt.Errorf("error with bind consumer and queue: %s", err.Error())
	}
	return nil
}

func (c *Consumer) Reconnect() error {
	// if connection, channel not closed
	if c.conn.IsClosed() {
		err := c.createConnection()
		if err != nil {
			return err
		}

		err = c.openChannel()

		if err != nil {
			return err
		}
	}

	if c.channel.IsClosed() {
		err := c.openChannel()
		if err != nil {
			return err
		}
	}

	// bind exchange consumer and bind
	err := c.bind()
	if err != nil {
		return err
	}

	go c.handleClosedConnAndChannel()
	return nil
}

// RestartHandle Experimental!!!  if we get an error, then we reopen the connection and the channel and the work of the handler
func (c *Consumer) RestartHandle() error {
	err := c.Reconnect()
	err = c.Run()
	return err
}

//// Handling channel closing and connection closing
func (c *Consumer) handleClosedConnAndChannel() {
	var hasErr bool
loop:
	for {
		select {
		case <-c.conn.NotifyClose(make(chan *amqp.Error)):
			hasErr = true
			break loop
		case <-c.channel.NotifyClose(make(chan *amqp.Error)):
			hasErr = true
			break loop
		case <-c.done:
			break loop
		}
	}

	//// If closed channel or rabbitmq
	if hasErr {
		c.ErrorCh <- ErrClosed
	}
}

func (c *Consumer) GetChannel() *amqp.Channel {
	return c.channel
}

func (c *Consumer) GetConnection() *amqp.Connection {
	return c.conn
}

func (c *Consumer) Run() error {
	// Exec consumer for consumer
	deliveries, err := c.channel.Consume(
		c.queueName,
		c.tag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return errors.New(fmt.Sprintf("error with run handle messages: %s", err.Error()))
	}

	go c.handle(deliveries)

	return nil
}

// Shutdown Off consumer by tag
func (c *Consumer) Shutdown() error {
	c.done <- struct{}{}
	if !c.channel.IsClosed() {
		if err := c.channel.Cancel(c.tag, true); err != nil {
			return fmt.Errorf("consumer cancel failed: %s", err)
		}
	}
	if !c.conn.IsClosed() {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}
	defer log.Printf("AMQP shutdown OK")
	return nil
}
