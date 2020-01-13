package queue

import (
	"fmt"
	"testing"
	"time"
)

type Message struct {
	Data string `json:"data"`
}

func TestPutMessage(t *testing.T) {
	broker := New()
	defer broker.Close()
	ok := broker.CreateQueue("testing", 100)
	if !ok {
		t.Fail()
	}
	go broker.Start()
	subcriber, err := broker.CreateSubscriber("testing")
	if err != nil {
		t.Fail()
	}
	go subcriber.Subcsribe(subcribeFunc1)
	subcriber2, err := broker.CreateSubscriber("testing")
	if err != nil {
		t.Fail()
	}
	go subcriber2.Subcsribe(subcribeFunc2)
	err = broker.Put("testing", Message{Data: "Hellow World"})
	if err != nil {
		t.Fail()
	}

	err = broker.Put("testing", Message{Data: "Ini message ke dua"})
	if err != nil {
		t.Fail()
	}
	fmt.Println("DONE DISINI")
	time.Sleep(1000000)
	broker.RemoveSubscriber("testing", subcriber)
	err = broker.Put("testing", Message{Data: "Hellow World after remove"})
	if err != nil {
		t.Fail()
	}
	time.Sleep(1000000)
}

func subcribeFunc1(data interface{}) error {
	msg := data.(Message)
	fmt.Println("1: " + msg.Data)
	return nil
}

func subcribeFunc2(data interface{}) error {
	msg := data.(Message)
	fmt.Println("2: " + msg.Data)
	return nil
}
