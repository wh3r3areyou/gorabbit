package gorabbit

type Config struct {
	Connection   string
	QueueName    string
	ExchangeName string
	Tag          string

	// Use goroutines for handling and publish message
	Sync bool
}
