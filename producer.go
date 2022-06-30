package gorabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type (
	Producer struct {
		sync          bool
		queueName     string
		exchangeName  string
		exchangeType  string
		routingKey    string
		tag           string
		connectionStr string
		done          chan struct{}
		ErrorCh       chan error
		conn          *amqp.Connection
		channel       *amqp.Channel
	}
)

// NewProducer Initialize producer
func NewProducer(config Config) (*Producer, error) {
	p := Producer{
		queueName:     config.QueueName,
		exchangeName:  config.ExchangeName,
		exchangeType:  config.TypeExchange,
		routingKey:    config.RoutingKey,
		connectionStr: config.Connection,
		sync:          config.Sync,
		done:          make(chan struct{}),
		ErrorCh:       make(chan error),
	}

	var err error
	// create connection with rabbitmq
	err = p.createConnection()
	if err != nil {
		return nil, err
	}

	// get channel
	err = p.openChannel()
	if err != nil {
		return nil, err
	}

	// bind exchange consumer and bind
	err = p.bind()
	if err != nil {
		return nil, err
	}

	go p.handleClosedConnAndChannel()

	return &p, nil
}

// create connection with rabbitmq
func (p *Producer) createConnection() error {
	var err error
	p.conn, err = amqp.Dial(p.connectionStr)
	if err != nil {
		return fmt.Errorf("error with create connection with rabbitmq: %s", err.Error())
	}
	return nil
}

// open channel in rabbitmq for producer
func (p *Producer) openChannel() error {
	var err error
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("error with openChannel with rabbitmq: %s", err.Error())
	}
	return nil
}

// bind exchange consumer and queue
func (p *Producer) bind() error {
	err := bind(p.channel, p.exchangeName, p.exchangeType, p.routingKey, p.queueName)
	if err != nil {
		return fmt.Errorf("error with bind consumer and queue: %s", err.Error())
	}
	return nil
}

//// Handling channel closing and connection closing
func (p *Producer) handleClosedConnAndChannel() {
	var hasErr bool
loop:
	for {
		select {
		case <-p.conn.NotifyClose(make(chan *amqp.Error)):
			hasErr = true
			break loop
		case <-p.channel.NotifyClose(make(chan *amqp.Error)):
			hasErr = true
			break loop
		case <-p.done:
			break loop
		}
	}
	//// If closed channel or rabbitmq
	if hasErr {
		p.ErrorCh <- ErrClosed
	}
}

// Send message
func (p *Producer) publish(message []byte) error {
	if err := p.channel.Publish(
		p.exchangeName,
		p.routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            message,
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		},
	); err != nil {
		return err
	}

	return nil
}

// Publish Send message to queue
func (p *Producer) Publish(message []byte) {
	if p.sync {
		err := p.publish(message)
		if err != nil {
			if err != ErrClosed {
				p.ErrorCh <- err
			}
		}
	} else {
		go func() {
			err := p.publish(message)
			if err != nil {
				if err != ErrClosed {
					p.ErrorCh <- err
				}
			}
		}()
	}
}

func (p *Producer) Reconnect() error {
	// if connection, channel not closed
	if p.conn.IsClosed() {
		err := p.createConnection()
		if err != nil {
			return err
		}

		err = p.openChannel()

		if err != nil {
			return err
		}
	}

	if p.channel.IsClosed() {
		err := p.openChannel()
		if err != nil {
			return err
		}
	}

	// bind producer and queue
	err := p.bind()
	if err != nil {
		return err
	}

	go p.handleClosedConnAndChannel()

	return nil
}

func (p *Producer) GetChannel() *amqp.Channel {
	return p.channel
}

func (p *Producer) GetConnection() *amqp.Connection {
	return p.conn
}

// Shutdown Off producer by tag
func (p *Producer) Shutdown() error {
	p.done <- struct{}{}
	if !p.channel.IsClosed() {
		if err := p.channel.Cancel(p.tag, true); err != nil {
			return fmt.Errorf("producer cancel failed: %s", err)
		}
	}

	if !p.conn.IsClosed() {
		if err := p.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	defer log.Printf("AMQP shutdown OK")
	return nil
}
