package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"time"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6653", //支持："pulsar://localhost:6650,localhost:6651,localhost:6652"
		OperationTimeout:  60 * time.Second,
		ConnectionTimeout: 60 * time.Second,
	})

	defer client.Close()

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	d := &pulsar.DLQPolicy{
		MaxDeliveries:    3,
		RetryLetterTopic: "my-topic-RETRY",
		DeadLetterTopic:  "my-topic-DLQ",
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:               "my-topic",
		SubscriptionName:    "my-sub",
		Type:                pulsar.Shared,
		RetryEnable:         true,
		DLQ:                 d,
		NackRedeliveryDelay: time.Second * 3,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		err = consumer.Ack(msg)
		if err != nil {
			return
		}
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
}
