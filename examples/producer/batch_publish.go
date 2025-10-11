package main

///生产者示例：带消息确认的批量发送
import (
	"context"
	"fmt"
	"time"

	"github.com/s84662355/amqp/producer"
	"github.com/streadway/amqp"
)

func main() {
	// 初始化连接池（3个连接，每个连接10个通道）
	pool, err := producer.NewPool(
		3,  // 连接数
		10, // 每个连接的通道数
		"amqp://guest:guest@localhost:5672/",
		amqp.Config{
			Heartbeat: 30 * time.Second, // 心跳检测
		},
	)
	if err != nil {
		panic(fmt.Sprintf("连接池初始化失败: %v", err))
	}
	defer pool.Stop() // 程序退出时关闭连接池

	// 批量发送100条消息
	for i := 0; i < 100; i++ {
		// 使用上下文设置5秒超时
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 提交消息发布任务
		msgID := fmt.Sprintf("msg-%d", i)
		err := pool.Put(ctx, func(ch producer.Channel) error {
			// 声明交换机（如已存在则无影响）
			if err := ch.ExchangeDeclare(
				"logs",  // 交换机名称
				"topic", // 类型：topic支持通配符路由
				true,    // 持久化
				false,   // 不自动删除
				false,   // 非内部交换机
				false,   // 不等待确认
				nil,
			); err != nil {
				return fmt.Errorf("交换机声明失败: %v", err)
			}

			// 发布消息（带消息确认机制）
			return ch.Publish(
				"logs",          // 交换机
				"order.created", // 路由键
				false,           // 非必须路由
				false,           // 非立即投递
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte(fmt.Sprintf("订单创建消息: %s", msgID)),
					MessageId:    msgID,
					Timestamp:    time.Now(),
					DeliveryMode: amqp.Persistent, // 持久化消息
				},
			)
		})

		if err != nil {
			fmt.Printf("消息 %s 发送失败: %v\n", msgID, err)
		} else {
			fmt.Printf("消息 %s 发送成功\n", msgID)
		}
	}
}
