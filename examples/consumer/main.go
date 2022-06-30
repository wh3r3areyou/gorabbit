package main

import (
	"fmt"
	"github.com/wh3r3areyou/gorabbit"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Login, Password, Host, Port
	con := fmt.Sprintf("amqp://%s:%s@%s:%s/", "login", "passwd", "localhost", "5672")

	config := gorabbit.Config{
		Connection:   con,
		QueueName:    "messages",
		ExchangeName: "messages-exchange",
		TypeExchange: gorabbit.Direct,
		RoutingKey:   "messages-key",
		Tag:          "messages-consumer",
		Sync:         true,
	}

	consumer, err := gorabbit.NewConsumer(config, HandleMessage)
	defer consumer.Shutdown()
	if err != nil {
		panic(err)
	}

	err = consumer.Run()
	if err != nil {
		panic(err)
	}

	go handleErrors(consumer)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Print("Consumer stop")
}

func HandleMessage(message []byte) {
	log.Println(string(message))
}

func handleErrors(consumer *gorabbit.Consumer) {
	for vErr := range consumer.ErrorCh {
		if vErr == gorabbit.ErrClosed {
			err := consumer.RestartHandle()
			if err != nil {
				log.Println(err)
			}
		}
	}
}
