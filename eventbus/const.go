package eventbus

import "time"

// Supscription 重试与超时常量
const (
	defaultAddSubscriptionTimeout    = time.Duration(2) * time.Second
	defaultRemoveSubscriptionTimeout = time.Duration(2) * time.Second
	defaultAddSubscriptionRetry      = 5 // 尝试添加subscription最大cas尝试次数
	defaultRemoveSubscriptionRetry   = 5 // 尝试删除subscription最大cas尝试次数
	defaultLazyRemoveSubRetry        = 5 // 惰性删除每次lookup尝试次数
)

const (
	defaultSubscriptionQueueSize = 10
)

// SubConcurrencyMode 不同Publish之间对于Subscription的执行模式
type SubConcurrencyMode uint8

const (
	SubSerial SubConcurrencyMode = iota
	SubParallel
)

// SubscriptionFlag 回调触发标志位
type SubscriptionFlag uint8

const (
	FlagNone SubscriptionFlag = 0
	FlagOnce SubscriptionFlag = 1 << iota // 限制只能执行一次
)

// SubmitFailurePolicy 提交Subscription任务失败后的策略
type SubmitFailurePolicy uint8

const (
	SubmitFailureContinue SubmitFailurePolicy = iota // 其他Subscription继续执行
	SubmitFailureAbort                               // 其他Subscription停止执行
)

const (
	// defaultSnowflakeNode 雪花算法默认初始化Node使用
	defaultSnowflakeNode = 22
	// defaultGoPoolSize 执行器协程池默认大小
	defaultGoPoolSize = 100
)
