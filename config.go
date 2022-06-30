package gorabbit

type Config struct {
	Connection   string
	QueueName    string
	ExchangeName string
	Tag          string
	RoutingKey   string
	TypeExchange string

	// Use goroutines for handling and publish message
	Sync bool
}
