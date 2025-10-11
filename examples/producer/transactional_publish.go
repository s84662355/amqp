// examples/producer/transactional_publish.go
package main

///使用连接池发布事务消息

import (
	"context"
	"fmt"

	"github.com/s84662355/amqp/producer"
	"github.com/streadway/amqp"
)

func main() {
	pool, err := producer.NewPool(1, 5, "amqp://guest:guest@localhost:5672/", amqp.Config{})
	if err != nil {
		panic(err)
	}
	defer pool.Stop()

	// 提交事务消息任务
	err = pool.Put(context.Background(), func(ch producer.Channel) error {
		// 开启事务
		if err := ch.Tx(); err != nil {
			return fmt.Errorf("开启事务失败: %v", err)
		}
		defer func() {
			// 事务回滚（如果后续操作失败）
			if r := recover(); r != nil {
				ch.TxRollback()
				fmt.Println("事务回滚:", r)
			}
		}()

		// 第一步：发送订单消息
		if err := ch.Publish(
			"orders",
			"order.paid",
			false,
			false,
			amqp.Publishing{Body: []byte("订单已支付")},
		); err != nil {
			ch.TxRollback()
			return fmt.Errorf("订单消息发送失败: %v", err)
		}

		// 第二步：发送库存扣减消息
		if err := ch.Publish(
			"inventory",
			"stock.deduct",
			false,
			false,
			amqp.Publishing{Body: []byte("库存已扣减")},
		); err != nil {
			ch.TxRollback()
			return fmt.Errorf("库存消息发送失败: %v", err)
		}

		// 提交事务
		if err := ch.TxCommit(); err != nil {
			return fmt.Errorf("事务提交失败: %v", err)
		}
		fmt.Println("事务消息全部发送成功")
		return nil
	})
	if err != nil {
		fmt.Printf("任务执行失败: %v\n", err)
	}
}
