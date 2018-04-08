package async

import (
	"bytes"
	"encoding/gob"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/bitly/go-nsq"
)

type Task struct {
	name string
	// 暂时没有用到
	argType    []reflect.Type
	returnType []reflect.Type
	function   reflect.Value
}

func NewTask(f interface{}) Task {
	funcValue := reflect.ValueOf(f)

	rawName := runtime.FuncForPC(funcValue.Pointer()).Name()
	rawNameSplit := strings.Split(rawName, ".")
	funcName := rawNameSplit[len(rawNameSplit)-1]

	var argType []reflect.Type
	for i := 0; i < funcValue.Type().NumIn(); i++ {
		argType = append(argType, funcValue.Type().In(i))
	}

	var returnType []reflect.Type
	for i := 0; i < funcValue.Type().NumOut(); i++ {
		returnType = append(returnType, funcValue.Type().Out(i))
	}
	return Task{funcName, argType, returnType, funcValue}
}

type TaskHub struct {
	mu     sync.Mutex
	tasks  map[string]Task
	config *nsq.Config
}

func NewTaskHub(config *nsq.Config) *TaskHub {
	return &TaskHub{tasks: map[string]Task{}, config: config}
}

func (th *TaskHub) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		log.Panic("empty body of task")
	}

	codec := newgobCodec()
	request, err := codec.decode(m.Body)
	if err != nil {
		log.Panic(err)
	}

	if handler, ok := th.tasks[request.FuncName]; ok {
		var args []reflect.Value
		for _, v := range request.Args {
			value := reflect.ValueOf(v)
			args = append(args, value)
		}
		handler.function.Call(args)
	}
	return nil
}

func (th *TaskHub) Consume(nsqld string) {
	consumer, _ := nsq.NewConsumer("write_test", "ch_test", th.config)
	consumer.ChangeMaxInFlight(200)
	consumer.AddConcurrentHandlers(th, 20)
	err := consumer.ConnectToNSQLookupd(nsqld)
	if err != nil {
		log.Panic(err)
	}
}

func (th *TaskHub) Register(name string, f interface{}) Task {
	th.mu.Lock()
	defer th.mu.Unlock()

	task := NewTask(f)
	if name != "" {
		th.tasks[name] = task
	} else {
		th.tasks[task.name] = task
	}
	return task
}

type Request struct {
	FuncName string
	Args     []interface{}
}

func Produce(handler_name string, args ...interface{}) {
	config := nsq.NewConfig()
	w, _ := nsq.NewProducer("127.0.0.1:4150", config)
	rq := Request{handler_name, args}
	codec := newgobCodec()
	b, err := codec.encode(rq)
	if err != nil {
		log.Panic(err)
	}
	err = w.Publish("write_test", b)
	if err != nil {
		log.Panic("could not connect")
	}
	w.Stop()
}

type gobCodec struct {
	req Request
	enc *gob.Encoder
	dec *gob.Decoder
	buf bytes.Buffer
}

func newgobCodec() *gobCodec {
	codec := &gobCodec{}
	var buf = bytes.Buffer{}
	codec.buf = buf
	codec.enc = gob.NewEncoder(&codec.buf)
	codec.dec = gob.NewDecoder(&codec.buf)

	return codec
}

func (c *gobCodec) encode(req Request) ([]byte, error) {
	c.req = req
	err := c.enc.Encode(c.req)
	if err != nil {
		return nil, err
	}
	return c.buf.Bytes(), nil
}

func (c *gobCodec) decode(b []byte) (Request, error) {
	var buf bytes.Buffer
	// var req request

	_, err := buf.Write(b)
	if err != nil {
		return Request{}, err
	}

	c.buf = buf
	err = c.dec.Decode(&c.req)
	return c.req, err
}
