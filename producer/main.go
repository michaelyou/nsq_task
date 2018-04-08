package main

import (
	// "log"

	// "github.com/bitly/go-nsq"
	"nsq_p/async"
)

func main() {
	// config := nsq.NewConfig()
	// w, _ := nsq.NewProducer("127.0.0.1:4150", config)

	// err := w.Publish("write_test", []byte("test"))
	// if err != nil {
	// 	log.Panic("could not connect")
	// }
	// w.Stop()
	async.Produce("Minus", 5, 2)
	async.Produce("Add", 1, 2)
}
