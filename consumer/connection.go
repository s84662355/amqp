package consumer

import (
	"context"
	"fmt"
	"slices"
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
	mu          sync.RWMutex 
	status      bool
}

func NewConnection(
	url string,
	config amqp.Config,
) *Connection {
	conn := &Connection{
		config: config,
		url:    url,
	}
	conn.ctx, conn.cancel = context.WithCancel(context.Background())

	return conn
}

func (conn *Connection) AddTask(f ConsumerFunc) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	select {
	case <-conn.ctx.Done():
		return fmt.Errorf("连接已经关闭,不能再添加任务")
	default:
	}

	if conn.status {
		return fmt.Errorf("连接已经开启,不能再添加任务")
	}

	conn.channelPool = append(conn.channelPool, &channel{
		ctx: conn.ctx,
		f:   f,
	})

	return nil
}

func (conn *Connection) Start() {
	conn.stop.Do(func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		select {
		case <-conn.ctx.Done():
			return
		default:
		}

		conn.status = true

		conn.wg.Add(1)
		go func() {
			defer conn.wg.Done()
			conn.run()
		}()
	})
}

func (conn *Connection) Stop() {
	conn.stop.Do(func() {
		conn.mu.Lock()
		conn.cancel()
		conn.mu.Unlock()
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
					notifyClose := aconn.NotifyClose(make(chan *amqp.Error, 1))
					conn.runChannel(aconn, notifyClose)
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

func (conn *Connection) runChannel(c *amqp.Connection, notifyClose chan *amqp.Error) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for _, ch := range conn.channelPool {
		if !ch.isfinish {
			ch.conn = c
			ch.connNotifyCloseChan = notifyClose
			ch.ctx = conn.ctx
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch.run()

				if ch.isfinish {
					conn.mu.Lock()
					conn.channelPool = slices.DeleteFunc(conn.channelPool, func(n *channel) bool {
						return n == ch // delete
					})
					fmt.Println(conn.channelPool)
					conn.mu.Unlock()

				}
			}()
		}
	}
}
