package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	ErrConnClosedCannotAddTask      = errors.New("the connection has been closed, and no more tasks can be added")
	ErrConnAlreadyOpenCannotAddTask = errors.New("the connection is already open, and no more tasks can be added")
)

// Connection 管理RabbitMQ连接和消费者通道
// 实现了连接的创建、任务添加、启动消费和异常重连等功能
type Connection struct {
	ctx         context.Context    // 上下文，用于控制生命周期
	cancel      context.CancelFunc // 取消上下文的函数
	channelPool []*channel         // 消费者通道池，存储所有任务对应的通道
	config      amqp.Config        // RabbitMQ连接配置
	url         string             // 连接URL
	stop        sync.Once          // 确保Stop方法只执行一次
	mu          sync.RWMutex       // 互斥锁，保护并发访问
	status      bool               // 连接状态标识（是否已启动）
	done        chan struct{}      // 通知连接关闭的通道
}

// NewConnection 创建新的RabbitMQ连接管理器
// 参数:
//
//	url - RabbitMQ服务器连接地址
//	config - 连接配置（如认证信息、连接参数等）
func NewConnection(
	url string,
	config amqp.Config,
) *Connection {
	conn := &Connection{
		config: config,
		url:    url,
	}
	// 创建可取消的上下文，作为整个连接的生命周期控制
	conn.ctx, conn.cancel = context.WithCancel(context.Background())
	return conn
}

// AddTask 向连接管理器添加消费任务
// 参数:
//
//	f - 消费者函数，处理接收到的消息
//
// 返回:
//
//	error - 添加任务时的错误（如连接已关闭）
func (conn *Connection) AddTask(f ConsumerFunc) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// 检查上下文是否已取消（连接已关闭）
	select {
	case <-conn.ctx.Done():
		return ErrConnClosedCannotAddTask
	default:
	}

	// 检查连接是否已启动（启动后不允许添加新任务）
	if conn.status {
		return ErrConnAlreadyOpenCannotAddTask
	}

	// 创建新的消费者通道并添加到通道池
	conn.channelPool = append(conn.channelPool, &channel{
		ctx: conn.ctx,
		f:   f,
	})
	return nil
}

// Start 启动连接和所有消费者任务
// 返回:
//
//	<-chan struct{} - 连接关闭时通知的通道
func (conn *Connection) Start() <-chan struct{} {
	conn.stop.Do(func() {
		conn.mu.Lock()
		// 标记连接为已启动状态
		conn.status = true
		conn.done = make(chan struct{})
		conn.mu.Unlock()

		select {
		case <-conn.ctx.Done():
			close(conn.done)
			return
		default:
		}

		// 启动连接运行协程（在后台处理连接和重连逻辑）
		go func() {
			defer close(conn.done)
			conn.run()
		}()
	})
	return conn.done
}

// Stop 优雅关闭连接和所有消费者任务
// 确保资源释放和任务正常终止
func (conn *Connection) Stop() {
	conn.stop.Do(func() {
		conn.cancel()
		var done chan struct{}
		conn.mu.Lock()
		done = conn.done
		conn.mu.Unlock()
		if done != nil {
			<-done
		}
	})
}

// run 连接管理器的核心运行逻辑
// 负责创建RabbitMQ连接、处理重连和管理通道生命周期
func (conn *Connection) run() {
	for {
		select {
		case <-conn.ctx.Done():
			// 上下文取消时退出循环
			return
		default:
			// 创建RabbitMQ连接
			aconn, err := amqp.DialConfig(conn.url, conn.config)
			if err == nil {
				// 连接创建成功，启动通道运行协程
				done := make(chan struct{})
				go func() {
					defer close(done)
					// 监听连接关闭事件
					notifyClose := aconn.NotifyClose(make(chan *amqp.Error, 1))
					conn.runChannel(aconn, notifyClose)
				}()

				// 等待通道运行完成或上下文取消
				select {
				case <-done:
				case <-conn.ctx.Done():
				}
				// 关闭连接
				aconn.Close()

				// 等待所有通道协程完成
				for range done {
					/* code */
				}

			} else {
				// 连接创建失败时打印错误（实际应用中可添加重试策略）
				fmt.Println(err)
			}

			// 检查是否有需要处理的任务
			if len(conn.channelPool) == 0 {
				return
			}
			// 等待1秒后重试连接（避免频繁重连）
			ctx, _ := context.WithTimeout(conn.ctx, 1*time.Second)
			<-ctx.Done()
		}
	}
}

// runChannel 管理消费者通道的运行
// 参数:
//
//	c - RabbitMQ连接实例
//	notifyClose - 连接关闭通知通道
func (conn *Connection) runChannel(c *amqp.Connection, notifyClose chan *amqp.Error) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	// 为每个通道启动消费协程
	for _, ch := range conn.channelPool {
		if !ch.isfinish {
			// 绑定通道到连接和关闭通知
			ch.conn = c
			ch.connNotifyCloseChan = notifyClose
			ch.ctx = conn.ctx

			wg.Add(1)
			go func() {
				defer wg.Done()
				// 运行通道消费逻辑
				ch.run()

				// 通道完成消费后从池中移除
				if ch.isfinish {
					conn.mu.Lock()
					conn.channelPool = slices.DeleteFunc(conn.channelPool, func(n *channel) bool {
						return n == ch // 删除指定通道
					})
					conn.mu.Unlock()
				}
			}()
		}
	}
}
