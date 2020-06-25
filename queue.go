package queue

import (
	"fmt"
	"io"
	"log"
	"sync"
)

var defaultQueueBuffer = 1000

//MessageBroker embedded queue in a service
type MessageBroker interface {
	io.Closer
	Start()
	CreateQueue(queueName string, messageBuffer int) bool
	Put(queueName string, data interface{}) error
	CreateSubscriber(queueName string) (*Subcriber, error)
	RemoveSubscriber(queueName string, sub *Subcriber) (bool, error)
}

type embeddedBroker struct {
	queueMap sync.Map
	creator  chan *Queue
}

//Queue ...
type Queue struct {
	listener    chan interface{}
	leave       chan *Subcriber
	subscribers sync.Map
}

//Subcriber ...
type Subcriber struct {
	forward chan interface{}
}

//Subcsribe ...
func (s *Subcriber) Subcsribe(f func(interface{}) error) error {
	for d := range s.forward {
		err := f(d)
		if err != nil {
			log.Printf("Error subscribe: %s\n", err.Error())
		}
	}
	return nil
}

//Run ...
func (q *Queue) Run() error {
	for {
		select {
		case data := <-q.listener:
			q.subscribers.Range(func(key, value interface{}) bool {
				val, ok := key.(*Subcriber)
				if !ok {
					return false
				}
				val.forward <- data
				return true
			})
		case sub := <-q.leave:
			q.subscribers.Delete(sub)
		}
	}
}

func (q *Queue) addSubscriber(sub *Subcriber) {
	q.subscribers.Store(sub, true)
}

func (q *Queue) close() {
	q.subscribers.Range(func(key, value interface{}) bool {
		val, ok := key.(*Subcriber)
		if !ok {
			return false
		}
		close(val.forward)
		return true
	})
	close(q.listener)
	close(q.leave)
}

//New ...
func New() MessageBroker {
	return &embeddedBroker{
		queueMap: sync.Map{},
		creator:  make(chan *Queue, defaultQueueBuffer),
	}
}

func (e *embeddedBroker) Close() error {
	log.Printf("Clear All memory")
	e.queueMap.Range(func(key, value interface{}) bool {
		val, ok := value.(*Queue)
		if !ok {
			return false
		}
		val.close()
		return true
	})
	close(e.creator)
	return nil
}

func (e *embeddedBroker) Start() {
	for q := range e.creator {
		go q.Run()
	}
}

func (e *embeddedBroker) RemoveSubscriber(queueName string, sub *Subcriber) (bool, error) {
	q, ok := e.queueMap.Load(queueName)
	if !ok {
		return false, fmt.Errorf("Queue %s not found", queueName)
	}
	theQueue := q.(*Queue)
	theQueue.leave <- sub
	return true, nil
}

func (e *embeddedBroker) CreateQueue(queueName string, messageBuffer int) bool {
	if messageBuffer == 0 {
		//default message buffer 100
		messageBuffer = 100
	}
	_, ok := e.queueMap.Load(queueName)
	if !ok {
		queue := &Queue{
			listener:    make(chan interface{}, messageBuffer),
			subscribers: sync.Map{},
			leave:       make(chan *Subcriber),
		}
		e.queueMap.Store(queueName, queue)
		e.creator <- queue
	}
	return true
}

func (e *embeddedBroker) Put(queueName string, data interface{}) error {
	q, ok := e.queueMap.Load(queueName)
	if !ok {
		return fmt.Errorf("Queue %s not found", queueName)
	}
	theQueue := q.(*Queue)
	theQueue.listener <- data
	return nil
}
func (e *embeddedBroker) CreateSubscriber(queueName string) (*Subcriber, error) {
	q, ok := e.queueMap.Load(queueName)
	if !ok {
		return nil, fmt.Errorf("Queue %s not found", queueName)
	}
	theQueue := q.(*Queue)
	subscriber := &Subcriber{
		forward: make(chan interface{}),
	}
	theQueue.addSubscriber(subscriber)
	return subscriber, nil
}
