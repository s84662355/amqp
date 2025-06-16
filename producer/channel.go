package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type ChannelTaskFunc func(c Channel) error

type ChannelTask struct {
	f     ChannelTaskFunc
	isRun atomic.Bool
	res   chan error
}

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

type Ch struct {
	achannel *amqp.Channel
}

func (ch *Ch) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *Ch) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *Ch) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return ch.achannel.ExchangeDelete(name, ifUnused, noWait)
}

func (ch *Ch) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeBind(destination, key, source, noWait, args)
}

func (ch *Ch) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	return ch.achannel.ExchangeUnbind(destination, key, source, noWait, args)
}

func (ch *Ch) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	return ch.achannel.NotifyConfirm(ack, nack)
}

func (ch *Ch) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return ch.achannel.NotifyPublish(confirm)
}

func (ch *Ch) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.achannel.Publish(exchange, key, mandatory, immediate, msg)
}

func (ch *Ch) Tx() error {
	return ch.achannel.Tx()
}

func (ch *Ch) TxCommit() error {
	return ch.achannel.TxCommit()
}

func (ch *Ch) TxRollback() error {
	return ch.achannel.TxRollback()
}

func (ch *Ch) Confirm(noWait bool) error {
	return ch.achannel.Confirm(noWait)
}

type channel struct {
	conn                *amqp.Connection
	taskChan            <-chan *ChannelTask
	connNotifyCloseChan <-chan *amqp.Error
	ctx                 context.Context
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
	for {
		select {
		case <-ch.ctx.Done():
			return
		case <-notifyClose:
			return
		case t, ok := <-ch.taskChan:
			if ok && t != nil {
				if !t.isRun.CompareAndSwap(false, true) {
					continue
				}
				t.res <- t.f(c)
			} else {
				return
			}
		}
	}
}
