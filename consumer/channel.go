package consumer

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

// ConsumerFunc 定义消费者处理函数类型
// 参数:
//
//	c - 通道接口实例，用于操作RabbitMQ
//
// 返回:
//
//	bool - 处理完成标识（true表示任务完成，可从池中移除）
type ConsumerFunc func(c Channel) bool

// Channel 定义RabbitMQ通道操作接口
// 封装了RabbitMQ通道的核心操作，便于测试和扩展
type Channel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Cancel(consumer string, noWait bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueInspect(name string) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	QueuePurge(name string, noWait bool) (int, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error)
	Recover(requeue bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
}

// Ch 实现Channel接口，封装amqp.Channel
// 将amqp原生通道包装为自定义接口，便于解耦和扩展
type Ch struct {
	achannel *amqp.Channel // 底层amqp通道实例
}

// Qos 设置通道的QoS（服务质量）
// 控制消费者每次从服务器获取的消息数量
func (ch *Ch) Qos(prefetchCount, prefetchSize int, global bool) error {
	return ch.achannel.Qos(prefetchCount, prefetchSize, global)
}

// Cancel 取消消费者订阅
func (ch *Ch) Cancel(consumer string, noWait bool) error {
	return ch.achannel.Cancel(consumer, noWait)
}

// QueueDeclare 声明队列（若不存在则创建）
// 参数:
//
//	name - 队列名称
//	durable - 是否持久化
//	autoDelete - 是否自动删除
//	exclusive - 是否排他
//	noWait - 是否等待服务器确认
//	args - 额外参数
func (ch *Ch) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// QueueDeclarePassive 被动声明队列（仅检查队列是否存在）
func (ch *Ch) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

// QueueInspect 检查队列状态
func (ch *Ch) QueueInspect(name string) (amqp.Queue, error) {
	return ch.achannel.QueueInspect(name)
}

// QueueBind 将队列绑定到交换机
func (ch *Ch) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return ch.achannel.QueueBind(name, key, exchange, noWait, args)
}

// QueueUnbind 解除队列与交换机的绑定
func (ch *Ch) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return ch.achannel.QueueUnbind(name, key, exchange, args)
}

// QueuePurge 清空队列中的消息
func (ch *Ch) QueuePurge(name string, noWait bool) (int, error) {
	return ch.achannel.QueuePurge(name, noWait)
}

// QueueDelete 删除队列
func (ch *Ch) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return ch.achannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// Consume 开始消费队列中的消息
// 返回:
//
//	<-chan amqp.Delivery - 消息通道
func (ch *Ch) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.achannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

// Get 从队列中获取一条消息
func (ch *Ch) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	return ch.achannel.Get(queue, autoAck)
}

// Recover 恢复未确认的消息
func (ch *Ch) Recover(requeue bool) error {
	return ch.achannel.Recover(requeue)
}

// Ack 确认消息已处理
func (ch *Ch) Ack(tag uint64, multiple bool) error {
	return ch.achannel.Ack(tag, multiple)
}

// Nack 否定确认消息（可选择重新入队）
func (ch *Ch) Nack(tag uint64, multiple bool, requeue bool) error {
	return ch.achannel.Nack(tag, multiple, requeue)
}

// Reject 拒绝消息（可选择重新入队）
func (ch *Ch) Reject(tag uint64, requeue bool) error {
	return ch.achannel.Reject(tag, requeue)
}

// channel 消费者通道实现
// 管理通道的生命周期和消费逻辑
type channel struct {
	conn                *amqp.Connection   // RabbitMQ连接实例
	connNotifyCloseChan <-chan *amqp.Error // 连接关闭通知通道
	ctx                 context.Context    // 上下文，控制通道生命周期
	f                   ConsumerFunc       // 消费者处理函数
	isfinish            bool               // 通道完成标识
}

// run 通道核心运行逻辑
// 负责创建通道、执行消费任务和处理异常重连
func (ch *channel) run() {
	for {
		select {
		case <-ch.ctx.Done():
			// 上下文取消时退出
			return
		case <-ch.connNotifyCloseChan:
			// 连接关闭时退出
			return
		default:
			// 创建RabbitMQ通道
			achannel, err := ch.conn.Channel()
			if err == nil {
				// 执行消费任务
				ch.runTask(achannel)
				// 任务完成后关闭通道
				achannel.Close()
			}

			// 检查通道是否已完成
			if ch.isfinish {
				return
			}

			// 等待1秒后重试（避免频繁重连）
			ctx, c := context.WithTimeout(ch.ctx, 1*time.Second)
			<-ctx.Done()
			c()
		}
	}
}

// runTask 执行具体的消费任务
// 参数:
//
//	achannel - 底层amqp通道实例
func (ch *channel) runTask(achannel *amqp.Channel) {
	// 监听通道关闭事件
	notifyClose := achannel.NotifyClose(make(chan *amqp.Error, 1))
	// 创建自定义通道接口实例
	c := &Ch{
		achannel: achannel,
	}

	done := make(chan struct{})
	defer func() {
		// 等待任务完成通知
		for range done {
			/* code */
		}
	}()
	// 启动消费任务协程
	go func() {
		defer close(done)
		// 执行消费者处理函数，获取完成标识
		ch.isfinish = ch.f(c)
	}()

	// 等待上下文取消、通道关闭或任务完成
	select {
	case <-ch.ctx.Done():
	case <-notifyClose:
	case <-done:
	}

	// 关闭通道
	achannel.Close()
}
