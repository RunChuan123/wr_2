package main

import (
	"flag"
	"github.com/gin-gonic/gin"
	"wr_2/router"
)

func main() {
	// 指定端口
	port := flag.String("p", "8080", "http port")
	flag.Parse()
	// goroutine 处理gossip
	go router.HandleGossip(*port)
	go router.ExpirationMonitor()
	r := gin.Default()
	// 注册路由
	r = router.InitRouter(r)
	r.Run(":" + *port)
}
