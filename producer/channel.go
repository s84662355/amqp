package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// ChannelTaskFunc 定义生产者任务函数类型
// 参数:
//
//	c - 通道接口实例，用于发送消息
//
// 返回:
//
//	error - 任务执行错误
type ChannelTaskFunc func(c Channel) error

// ChannelTask 封装生产者任务
// 用于在通道中传递和执行消息发送任务
type ChannelTask struct {
	f     ChannelTaskFunc // 任务执行函数
	isRun atomic.Bool     // 任务执行状态标识（避免并发执行）
	res   chan error      // 任务执行结果通道
}

// Channel 定义RabbitMQ生产者通道操作接口
// 封装了RabbitMQ通道的核心操作，便于测试和扩展
type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error
	NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64)
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Tx() error
	TxCommit() error
	TxRollback() error
	Confirm(noWait bool) error
}

// Ch 实现Channel接口，封装amqp.Channel
// 将amqp原生通道包装为自定义接口，便于解耦和扩展
type Ch struct {
	achannel *amqp.Channel // 底层amqp通道实例
}

// ExchangeDeclare 声明交换机（若不存在则创建）
func (ch *Ch) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// ExchangeDeclarePassive 被动声明交换机（仅检查是否存在）
func (ch *Ch) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

// ExchangeDelete 删除交换机
func (ch *Ch) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return ch.achannel.ExchangeDelete(name, ifUnused, noWait)
}

// ExchangeBind 绑定交换机到交换机
func (ch *Ch) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeBind(destination, key, source, noWait, args)
}

// ExchangeUnbind 解除交换机绑定
func (ch *Ch) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeUnbind(destination, key, source, noWait, args)
}

// NotifyConfirm 注册确认和否定确认通道
// 用于监听消息发送的确认状态
func (ch *Ch) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	return ch.achannel.NotifyConfirm(ack, nack)
}

// NotifyPublish 注册发布确认通道
func (ch *Ch) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return ch.achannel.NotifyPublish(confirm)
}

// Publish 发送消息到交换机
func (ch *Ch) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.achannel.Publish(exchange, key, mandatory, immediate, msg)
}

// Tx 开启事务
func (ch *Ch) Tx() error {
	return ch.achannel.Tx()
}

// TxCommit 提交事务
func (ch *Ch) TxCommit() error {
	return ch.achannel.TxCommit()
}

// TxRollback 回滚事务
func (ch *Ch) TxRollback() error {
	return ch.achannel.TxRollback()
}

// Confirm 开启发布确认模式
func (ch *Ch) Confirm(noWait bool) error {
	return ch.achannel.Confirm(noWait)
}

// channel 生产者通道实现
// 管理通道的生命周期和任务执行
type channel struct {
	conn                *amqp.Connection    // RabbitMQ连接实例
	taskChan            <-chan *ChannelTask // 任务通道，接收待执行的任务
	connNotifyCloseChan <-chan *amqp.Error  // 连接关闭通知通道
	ctx                 context.Context     // 上下文，控制通道生命周期
}

// run 通道核心运行逻辑
// 负责创建通道、执行任务和处理异常重连
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
				// 执行任务处理逻辑
				ch.runTask(achannel)
				// 任务完成后关闭通道
				achannel.Close()
			}

			// 等待1秒后重试（避免频繁重连）
			ctx, _ := context.WithTimeout(ch.ctx, 1*time.Second)
			<-ctx.Done()
		}
	}
}

// runTask 执行具体的任务处理逻辑
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

	// 循环处理任务通道中的任务
	for {
		select {
		case <-ch.ctx.Done():
			// 上下文取消时退出
			return
		case <-notifyClose:
			// 通道关闭时退出
			return
		case t, ok := <-ch.taskChan:
			// 接收任务
			if ok && t != nil {
				// 使用CAS操作确保任务只执行一次
				if !t.isRun.CompareAndSwap(false, true) {
					continue
				}
				// 执行任务并返回结果
				t.res <- t.f(c)
			} else {
				// 通道关闭时退出
				return
			}
		}
	}
}
