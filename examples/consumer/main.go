package main

import (
	"fmt"
	"time"

	"github.com/s84662355/amqp/consumer"
	"github.com/streadway/amqp"
)

func main() {
	connection := consumer.NewConnection("amqp://guest:guest@localhost:5672/", amqp.Config{})

	defer connection.Stop()

	connection.AddTask(func(ch consumer.Channel) bool {
		fmt.Println(ch.QueueDeclare(
			"task_queue", // 队列名称
			true,         // 持久化
			false,        // 自动删除
			false,        // 排他性
			false,        // 不等待服务器响应
			nil,          // 额外参数
		))
		// 声明队列（确保队列存在）

		return true
	})

	connection.Start()

	<-time.After(5 * time.Second)
}
