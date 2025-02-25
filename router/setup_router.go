// 注册路由

package router

import "github.com/gin-gonic/gin"

func InitRouter(r *gin.Engine) *gin.Engine {

	r.POST("/insert", InsertAndUpdate)
	r.GET("/search", Search)
	r.DELETE("/delete", Delete)
	r.GET("/count", Count)
	r.POST("/gossip/recv", GossipRecv)

	return r
}
