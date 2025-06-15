package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type Pool struct {
	connectionPool []*Connection
	taskQueue      *nqueue[*ChannelTask]
	done           chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	stop           sync.Once
}

func NewPool(
	connCount int,
	channelCount int,
	url string,
	config amqp.Config,
) *Pool {
	p := &Pool{
		taskQueue: newnqueue[*ChannelTask](),
		done:      make(chan struct{}),
	}
	tChan := make(chan *ChannelTask, 0)
	p.connectionPool = make([]*Connection, connCount)
	for i := 0; i < connCount; i++ {
		p.connectionPool[i] = NewConnection(channelCount, tChan, url, config)
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go func() {
		defer close(p.done)
		p.taskQueue.DequeueFunc(func(task *ChannelTask, isClose bool) bool {
			select {
			case <-p.ctx.Done():
				return false
			case tChan <- task:
			}
			return true
		})
	}()
	return p
}

func (p *Pool) Put(ctx context.Context, f channelTaskFunc) error {
	return p.put(ctx, &ChannelTask{
		f:   f,
		res: make(chan error),
	})
}

func (p *Pool) put(ctx context.Context, task *ChannelTask) error {
	if err := p.taskQueue.Enqueue(task); err != nil {
		return err
	}
	select {
	case <-p.ctx.Done():
		if task.isRun.CompareAndSwap(false, true) {
			return fmt.Errorf("连接池已经关闭")
		}
		err := <-task.res
		return err
	case <-ctx.Done():
		if task.isRun.CompareAndSwap(false, true) {
			return fmt.Errorf("处理超时")
		}
		err := <-task.res
		return err
	case err := <-task.res:
		return err
	}
}

func (p *Pool) Stop() {
	p.stop.Do(func() {
		p.taskQueue.Close()
		p.cancel()
		<-p.done
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		for _, conn := range p.connectionPool {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn.Stop()
			}()
		}
	})
}
