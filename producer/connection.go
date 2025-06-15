package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Connection struct {
	ctx         context.Context
	cancel      context.CancelFunc
	channelPool []*channel
	config      amqp.Config
	url         string
	wg          sync.WaitGroup
	stop        sync.Once
}

func NewConnection(
	count int,
	tChan chan *ChannelTask,
	url string,
	config amqp.Config,
) *Connection {
	conn := &Connection{
		config: config,
		url:    url,
	}
	conn.ctx, conn.cancel = context.WithCancel(context.Background())
	conn.channelPool = make([]*channel, count)
	for i := 0; i < count; i++ {
		conn.channelPool[i] = &channel{
			taskChan: tChan,
		}
	}

	conn.wg.Add(1)
	go func() {
		defer conn.wg.Done()
		conn.run()
	}()

	return conn
}

func (conn *Connection) Stop() {
	conn.stop.Do(func() {
		conn.cancel()
		conn.wg.Wait()
	})
}

func (conn *Connection) run() {
	for {
		select {
		case <-conn.ctx.Done():
			return
		default:
			aconn, err := amqp.DialConfig(conn.url, conn.config)
			if err == nil {

				done := make(chan struct{})
				go func() {
					defer close(done)
					conn.runChannel(aconn)
				}()

				select {
				case <-done:
				case <-conn.ctx.Done():
				}
				aconn.Close()

				for range done {
					/* code */
				}

			} else {
				fmt.Println(err)
			}

			ctx, _ := context.WithTimeout(conn.ctx, 1*time.Second)
			<-ctx.Done()
		}
	}
}

func (conn *Connection) runChannel(c *amqp.Connection) {
	notifyClose := c.NotifyClose(make(chan *amqp.Error, 1))
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(len(conn.channelPool))
	for _, ch := range conn.channelPool {
		ch.conn = c
		ch.connNotifyCloseChan = notifyClose
		ch.ctx = conn.ctx

		go func() {
			defer wg.Done()
			ch.run()
		}()

	}
}
