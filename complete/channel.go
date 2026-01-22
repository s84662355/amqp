package complete

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

// ChannelTaskFunc 定义生产者任务函数类型
type ChannelTaskFunc func(ctx context.Context, c Channel) error

// ChannelTask 封装生产者任务
type ChannelTask struct {
	f   ChannelTaskFunc
	res chan error
}

// Channel 定义RabbitMQ生产者通道操作接口
type Channel interface {
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyFlow(c chan bool) chan bool
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyCancel(c chan string) chan string
	NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64)
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation

	// 队列操作
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueInspect(name string) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueUnbind(name, key, exchange string, args amqp.Table) error
	QueuePurge(name string, noWait bool) (int, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)

	// 交换操作
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error

	// 消费操作
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Get(queue string, autoAck bool) (amqp.Delivery, bool, error)
	Cancel(consumer string, noWait bool) error

	// 确认操作
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error

	// 质量控制
	Qos(prefetchCount, prefetchSize int, global bool) error

	// 消息发布
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error

	// 事务
	Tx() error
	TxCommit() error
	TxRollback() error

	// 确认模式
	Confirm(noWait bool) error

	// 恢复
	Recover(requeue bool) error

	// 流控
	Flow(active bool) error

	// 关闭
	Close() error
}

// Ch 实现Channel接口，封装amqp.Channel
type Ch struct {
	achannel *amqp.Channel
}

// NewCh 创建新的Ch实例
func NewCh(achannel *amqp.Channel) *Ch {
	return &Ch{achannel: achannel}
}

// NotifyClose 监听通道关闭通知
func (ch *Ch) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return ch.achannel.NotifyClose(c)
}

// NotifyFlow 监听流控制通知
func (ch *Ch) NotifyFlow(c chan bool) chan bool {
	return ch.achannel.NotifyFlow(c)
}

// NotifyReturn 监听返回消息通知
func (ch *Ch) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	return ch.achannel.NotifyReturn(c)
}

// NotifyCancel 监听消费者取消通知
func (ch *Ch) NotifyCancel(c chan string) chan string {
	return ch.achannel.NotifyCancel(c)
}

// NotifyConfirm 注册确认和否定确认通道
func (ch *Ch) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	return ch.achannel.NotifyConfirm(ack, nack)
}

// NotifyPublish 注册发布确认通道
func (ch *Ch) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return ch.achannel.NotifyPublish(confirm)
}

// QueueDeclare 声明队列
func (ch *Ch) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// QueueDeclarePassive 被动声明队列
func (ch *Ch) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

// QueueInspect 检查队列状态
func (ch *Ch) QueueInspect(name string) (amqp.Queue, error) {
	return ch.achannel.QueueInspect(name)
}

// QueueBind 绑定队列到交换机
func (ch *Ch) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return ch.achannel.QueueBind(name, key, exchange, noWait, args)
}

// QueueUnbind 解绑队列
func (ch *Ch) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return ch.achannel.QueueUnbind(name, key, exchange, args)
}

// QueuePurge 清空队列
func (ch *Ch) QueuePurge(name string, noWait bool) (int, error) {
	return ch.achannel.QueuePurge(name, noWait)
}

// QueueDelete 删除队列
func (ch *Ch) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return ch.achannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// ExchangeDeclare 声明交换机
func (ch *Ch) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// ExchangeDeclarePassive 被动声明交换机
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

// Consume 消费消息
func (ch *Ch) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.achannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

// Get 获取单条消息
func (ch *Ch) Get(queue string, autoAck bool) (amqp.Delivery, bool, error) {
	return ch.achannel.Get(queue, autoAck)
}

// Cancel 取消消费者
func (ch *Ch) Cancel(consumer string, noWait bool) error {
	return ch.achannel.Cancel(consumer, noWait)
}

// Ack 确认消息
func (ch *Ch) Ack(tag uint64, multiple bool) error {
	return ch.achannel.Ack(tag, multiple)
}

// Nack 否定确认消息
func (ch *Ch) Nack(tag uint64, multiple bool, requeue bool) error {
	return ch.achannel.Nack(tag, multiple, requeue)
}

// Reject 拒绝消息
func (ch *Ch) Reject(tag uint64, requeue bool) error {
	return ch.achannel.Reject(tag, requeue)
}

// Qos 设置服务质量
func (ch *Ch) Qos(prefetchCount, prefetchSize int, global bool) error {
	return ch.achannel.Qos(prefetchCount, prefetchSize, global)
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

// Recover 恢复未确认的消息
func (ch *Ch) Recover(requeue bool) error {
	return ch.achannel.Recover(requeue)
}

// Flow 控制消息流
func (ch *Ch) Flow(active bool) error {
	return ch.achannel.Flow(active)
}

// Close 关闭通道
func (ch *Ch) Close() error {
	return ch.achannel.Close()
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
		case <-notifyClose:
			// 通道关闭时退出
			return
		default:
		}
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
				// 执行任务并返回结果
				t.res <- t.f(ch.ctx, c)
			} else {
				// 通道关闭时退出
				return
			}
		}
	}
}
