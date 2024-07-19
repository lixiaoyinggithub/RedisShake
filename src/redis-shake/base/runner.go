package base

var (
	Status      = "null"
	RDBPipeSize = 1024
)

// 不同操作类型的统一接口
type Runner interface {
	Main()

	GetDetailedInfo() interface{} // 返回结果类型不确定时，使用interface{}，相当于范型的效果
}
