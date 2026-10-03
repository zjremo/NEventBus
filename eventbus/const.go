package eventbus

import "time"

// Supscription 重试与超时常量
const (
	defaultAddSubscriptionTimeout    = time.Duration(2) * time.Second
	defaultRemoveSubscriptionTimeout = time.Duration(2) * time.Second
	defaultSubQueueSize              = 256
	defaultSubConsumerAliveTimeout   = time.Duration(5) * time.Second
	defaultCloseDrainTimeout         = time.Duration(5) * time.Second
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

const (
	// defaultSnowflakeNode 雪花算法默认初始化Node使用
	defaultSnowflakeNode = 22
	// defaultGoPoolSize 执行器协程池默认大小
	defaultGoPoolSize = 100
)
