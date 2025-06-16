package consumer

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type ConsumerFunc func(c Channel) bool

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

type Ch struct {
	achannel *amqp.Channel
}

func (ch *Ch) Qos(prefetchCount, prefetchSize int, global bool) error {
	return ch.achannel.Qos(prefetchCount, prefetchSize, global)
}

func (ch *Ch) Cancel(consumer string, noWait bool) error {
	return ch.achannel.Cancel(consumer, noWait)
}

func (ch *Ch) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *Ch) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return ch.achannel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *Ch) QueueInspect(name string) (amqp.Queue, error) {
	return ch.achannel.QueueInspect(name)
}

func (ch *Ch) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return ch.achannel.QueueBind(name, key, exchange, noWait, args)
}

func (ch *Ch) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return ch.achannel.QueueUnbind(name, key, exchange, args)
}

func (ch *Ch) QueuePurge(name string, noWait bool) (int, error) {
	return ch.achannel.QueuePurge(name, noWait)
}

func (ch *Ch) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return ch.achannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (ch *Ch) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.achannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (ch *Ch) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	return ch.achannel.Get(queue, autoAck)
}

func (ch *Ch) Recover(requeue bool) error {
	return ch.achannel.Recover(requeue)
}

func (ch *Ch) Ack(tag uint64, multiple bool) error {
	return ch.achannel.Ack(tag, multiple)
}

func (ch *Ch) Nack(tag uint64, multiple bool, requeue bool) error {
	return ch.achannel.Nack(tag, multiple, requeue)
}

func (ch *Ch) Reject(tag uint64, requeue bool) error {
	return ch.achannel.Reject(tag, requeue)
}

type channel struct {
	conn                *amqp.Connection
	connNotifyCloseChan <-chan *amqp.Error
	ctx                 context.Context
	f                   ConsumerFunc
	isfinish            bool
}

func (ch *channel) run() {
	for {
		select {
		case <-ch.ctx.Done():
			return
		case <-ch.connNotifyCloseChan:
			return
		default:

			achannel, err := ch.conn.Channel()
			if err == nil {
				ch.runTask(achannel)
				achannel.Close()
			}

			if ch.isfinish {
				return
			}

			ctx, _ := context.WithTimeout(ch.ctx, 1*time.Second)
			<-ctx.Done()

		}
	}
}

func (ch *channel) runTask(achannel *amqp.Channel) {
	notifyClose := achannel.NotifyClose(make(chan *amqp.Error, 1))
	c := &Ch{
		achannel: achannel,
	}

	done := make(chan struct{})
	defer func() {
		for range done {
			/* code */
		}
	}()
	go func() {
		defer close(done)

		ch.isfinish = ch.f(c)
	}()

	select {
	case <-ch.ctx.Done():

	case <-notifyClose:

	case <-done:

	}

	achannel.Close()
}
