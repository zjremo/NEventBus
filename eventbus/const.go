package eventbus

import "time"

// supscription 重试与超时常量
const (
	defaultAddSubscriptionTimeout    = time.Duration(2) * time.Second
	defaultRemoveSubscriptionTimeout = time.Duration(2) * time.Second
	defaultAddSubscriptionRetry      = 5 // 尝试添加subscription最大cas尝试次数
	defaultRemoveSubscriptionRetry   = 5 // 尝试删除subscription最大cas尝试次数
	defaultLazyRemoveSubRetry        = 5 // 惰性删除每次lookup尝试次数
)

// TopicConcurrencyMode Topic内部所有subscription的执行模式
type TopicConcurrencyMode uint8
const (
	TopicSerial TopicConcurrencyMode = iota
	TopicParallel 
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
	FlagOnce SubscriptionFlag = 1 << iota// 限制只能执行一次
)

// defaultSnowflakeNode 雪花算法默认初始化Node使用
const (
	defaultSnowflakeNode = 22
)
