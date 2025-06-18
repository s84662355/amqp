package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Connection 管理RabbitMQ生产者连接和通道
// 实现连接创建、任务分发和异常重连等功能
type Connection struct {
	ctx         context.Context    // 上下文，控制连接生命周期
	cancel      context.CancelFunc // 取消上下文的函数
	channelPool []*channel         // 通道池，存储多个生产者通道
	config      amqp.Config        // RabbitMQ连接配置
	url         string             // 连接URL
	wg          sync.WaitGroup     // 等待组，用于协程同步
	stop        sync.Once          // 确保Stop方法只执行一次
}

// NewConnection 创建新的RabbitMQ生产者连接管理器
// 参数:
//
//	count - 通道池大小（同时运行的生产者通道数）
//	tChan - 任务通道，用于接收待发送的消息任务
//	url - RabbitMQ服务器连接地址
//	config - 连接配置
//
// 返回:
//
//	*Connection - 连接管理器实例
//	error - 初始化错误（如count参数无效）
func NewConnection(
	count int,
	tChan chan *ChannelTask,
	url string,
	config amqp.Config,
) (*Connection, error) {
	// 检查通道池大小是否合法
	if count < 1 {
		return nil, fmt.Errorf("count 参数小于1")
	}

	conn := &Connection{
		config: config,
		url:    url,
	}
	// 创建可取消的上下文
	conn.ctx, conn.cancel = context.WithCancel(context.Background())
	// 初始化通道池
	conn.channelPool = make([]*channel, count)
	for i := 0; i < count; i++ {
		conn.channelPool[i] = &channel{
			taskChan: tChan, // 每个通道绑定任务通道
		}
	}

	// 启动连接运行协程（在后台处理连接和重连逻辑）
	conn.wg.Add(1)
	go func() {
		defer conn.wg.Done()
		conn.run()
	}()

	return conn, nil
}

// Stop 优雅关闭连接和所有通道
// 确保资源释放和任务正常终止
func (conn *Connection) Stop() {
	conn.stop.Do(func() {
		// 取消上下文，通知所有协程终止
		conn.cancel()
		// 等待连接运行协程完成
		conn.wg.Wait()
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
					conn.runChannel(aconn)
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

			// 等待1秒后重试连接（避免频繁重连）
			ctx, _ := context.WithTimeout(conn.ctx, 1*time.Second)
			<-ctx.Done()
		}
	}
}

// runChannel 管理生产者通道的运行
// 参数:
//
//	c - RabbitMQ连接实例
func (conn *Connection) runChannel(c *amqp.Connection) {
	// 监听连接关闭事件
	notifyClose := c.NotifyClose(make(chan *amqp.Error, 1))
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(len(conn.channelPool))

	// 为每个通道启动运行协程
	for _, ch := range conn.channelPool {
		ch.conn = c
		ch.connNotifyCloseChan = notifyClose
		ch.ctx = conn.ctx

		go func() {
			defer wg.Done()
			ch.run() // 运行通道发送逻辑
		}()
	}
}
