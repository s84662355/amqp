package producer

import (
	"context"
	"errors"
	"sync"

	"github.com/streadway/amqp"
)

var (
	ErrConnPoolClosed          = errors.New("The connection pool has been closed.")
	ErrConnCountLessThanOne    = errors.New("connCount parameter is less than 1")
	ErrChannelCountLessThanOne = errors.New("channelCount parameter is less than 1")
)

// Pool 管理RabbitMQ生产者连接池
// 实现连接池创建、任务提交和资源管理等功能
type Pool struct {
	connectionPool []*Connection         // 连接池，存储多个RabbitMQ连接
	taskQueue      *nqueue[*ChannelTask] // 任务队列，缓存待发送的消息任务
	done           chan struct{}         // 通知连接池关闭的通道
	ctx            context.Context       // 上下文，控制连接池生命周期
	cancel         context.CancelFunc    // 取消上下文的函数
	stop           sync.Once             // 确保Stop方法只执行一次
}

// NewPool 创建新的RabbitMQ生产者连接池
// 参数:
//
//	connCount - 连接池大小（同时保持的RabbitMQ连接数）
//	channelCount - 每个连接的通道数
//	url - RabbitMQ服务器连接地址
//	config - 连接配置
//
// 返回:
//
//	*Pool - 连接池实例
//	error - 初始化错误（如参数无效）
func NewPool(
	connCount int,
	channelCount int,
	url string,
	config amqp.Config,
) (*Pool, error) {
	// 检查连接池大小是否合法
	if connCount < 1 {
		return nil, ErrConnCountLessThanOne
	}

	// 检查通道数是否合法
	if channelCount < 1 {
		return nil, ErrChannelCountLessThanOne
	}

	p := &Pool{
		taskQueue: newnqueue[*ChannelTask](), // 初始化任务队列
		done:      make(chan struct{}),       // 初始化完成通道
	}
	// 创建任务通道（无缓冲，确保任务发送同步）
	tChan := make(chan *ChannelTask, 0)
	// 初始化连接池
	p.connectionPool = make([]*Connection, connCount)
	for i := 0; i < connCount; i++ {
		// 创建连接并添加到连接池
		conn, _ := NewConnection(channelCount, tChan, url, config)
		p.connectionPool[i] = conn
	}
	// 创建可取消的上下文
	p.ctx, p.cancel = context.WithCancel(context.Background())

	// 启动任务队列处理协程
	go func() {
		defer close(p.done)
		// 设置任务出队处理函数
		p.taskQueue.DequeueFunc(func(task *ChannelTask, isClose bool) bool {
			select {
			case <-p.ctx.Done():
				return false
			case tChan <- task: // 将任务发送到通道
			}
			return true
		})
	}()
	return p, nil
}

// Put 向连接池提交消息发送任务
// 参数:
//
//	ctx - 任务上下文（用于超时控制）
//	f - 任务执行函数
//
// 返回:
//
//	error - 任务执行错误
func (p *Pool) Put(ctx context.Context, f ChannelTaskFunc) error {
	return p.put(ctx, &ChannelTask{
		f:   f,
		res: make(chan error), // 创建任务结果通道
	})
}

// put 内部任务提交实现
func (p *Pool) put(ctx context.Context, task *ChannelTask) error {
	// 将任务入队
	if err := p.taskQueue.Enqueue(task); err != nil {
		return err
	}

	// 等待任务执行结果或上下文取消
	select {
	case <-p.ctx.Done():
		// 连接池关闭时处理任务
		if task.isRun.CompareAndSwap(false, true) {
			return ErrConnPoolClosed
		}
		err := <-task.res
		return err
	case <-ctx.Done():
		// 任务上下文超时
		if task.isRun.CompareAndSwap(false, true) {
			return ctx.Err()
		}
		err := <-task.res
		return err
	case err := <-task.res:
		// 获取任务执行结果
		return err
	}
}

// Stop 优雅关闭连接池
// 确保所有连接和任务正常终止
func (p *Pool) Stop() {
	p.stop.Do(func() {
		// 关闭任务队列
		p.taskQueue.Close()
		// 取消上下文，通知所有协程终止
		p.cancel()
		// 等待任务处理协程完成
		<-p.done

		wg := &sync.WaitGroup{}
		defer wg.Wait()
		// 关闭所有连接
		for _, conn := range p.connectionPool {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn.Stop()
			}()
		}
	})
}
