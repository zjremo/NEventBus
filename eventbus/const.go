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

// ConcurrencyMode 并发控制常量
type ConcurrencyMode uint8

const (
	ConcurSync ConcurrencyMode = 0
	ConcurAsync ConcurrencyMode = 1 << iota
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
