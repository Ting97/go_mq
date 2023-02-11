package main

import (
	"bytes"
	"context"
	"log"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/robfig/cron"
)

func main() {
	// 创建pulsar

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6653", //支持："pulsar://localhost:6650,localhost:6651,localhost:6652"
		OperationTimeout:  60 * time.Second,
		ConnectionTimeout: 60 * time.Second,
	})

	defer client.Close()

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	if err != nil {
		log.Fatal(err)
	}

	// 开始定时任务
	c := cron.New()
	// 0 0 14 ? * Mon 每周一 14点开始
	spec := "*/5 * * * * *" // 每隔5s执行一次，cron格式（秒，分，时，天，月，周）
	// 添加一个任务
	err = c.AddFunc(spec, func() {
		var buffer bytes.Buffer
		buffer.WriteString("hello time = ")
		buffer.WriteString(strconv.FormatInt(time.Now().Unix(), 10))
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: buffer.Bytes(),
		})
		log.Printf("111 time = %d\n", time.Now().Unix())
	})
	if err != nil {
		return
	}
	c.Start()

	defer producer.Close()
	defer c.Stop()
	select {}

}
