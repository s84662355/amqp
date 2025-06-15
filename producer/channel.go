package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type channelTaskFunc func(c Channel) error

type ChannelTask struct {
	f     channelTaskFunc
	isRun atomic.Bool
	res   chan error
}

type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Tx() error
	TxCommit() error
	TxRollback() error
	Confirm(noWait bool) error
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
				t.res <- t.f(achannel)
			} else {
				return
			}
		}
	}
}
