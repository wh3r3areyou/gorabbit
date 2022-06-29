package main

import (
	"fmt"
	"github.com/wh3r3areyou/gorabbit"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	// Login, Password, Host, Port
	con := fmt.Sprintf("amqp://%s:%s@%s:%s/", "login", "passwd", "localhost", "5672")

	config := gorabbit.Config{
		Connection:   con,
		QueueName:    "messages",
		ExchangeName: "messages-exchange",
		Tag:          "messages-producer",
		Sync:         true,
	}

	producer, err := gorabbit.NewProducer(config)
	defer producer.Shutdown()
	if err != nil {
		panic(err)
	}

	go handleErrors(producer)

	go func() {
		time.Sleep(5 * time.Second)
		producer.GetConnection().Close()
	}()

	for i := 0; i < 1000; i++ {
		producer.Publish([]byte(strconv.Itoa(i)))
		time.Sleep(1 * time.Second)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Print("Producer stop")
}

func handleErrors(producer *gorabbit.Producer) {
	for vErr := range producer.ErrorCh {
		if vErr == gorabbit.ErrClosed {
			err := producer.Reconnect()
			if err != nil {
				panic(err)
			}
		}
	}
}
