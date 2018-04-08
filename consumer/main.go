package main

import (
	"log"
	// "sync"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitly/go-nsq"
	"nsq_p/async"
)

func Add(a, b int) int {
	log.Println("result Add: ", a+b)
	return a + b
}

func Minus(a, b int) int {
	log.Println("Result Minus: ", a-b)
	return a - b
}

func main() {
	// wg := &sync.WaitGroup{}
	// wg.Add(1)

	config := nsq.NewConfig()
	// q, _ := nsq.NewConsumer("write_test", "ch", config)
	// q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
	// 	log.Printf("Got a message: %v", string(message.Body))
	// 	// wg.Done()
	// 	return nil
	// }))

	// err := q.ConnectToNSQLookupd("127.0.0.1:4161")
	// if err != nil {
	// 	log.Panic("could not connect")
	// }
	// wg.Wait()
	taskHub := async.NewTaskHub(config)
	taskHub.Register("", Add)
	taskHub.Register("", Minus)
	taskHub.Consume("127.0.0.1:4161")

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT)

	<-shutdown
}
