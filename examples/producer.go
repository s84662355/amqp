package main

import(
	"github.com/s84662355/amqp/producer"
	"github.com/streadway/amqp"
	"fmt"
	"context"
)

func main(){
   pool := producer.NewPool(2,5,"amqp://guest:guest@localhost:5672/",amqp.Config{})
   	defer pool.Stop()

   err:=pool. Put(context.Background(),func(ch producer.Channel)error{
        return ch.ExchangeDeclare(
		    "myExchange",  // 交换机名称
		    "direct",      // 类型
		    true,          // 持久化
		    false,         // 非自动删除
		    false,         // 非内部使用
		    false,         // 等待服务器确认
		    nil,           // 额外参数
		)
   })  
	 

   fmt.Println(err)
 
	 

}