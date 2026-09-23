package eventbus

import "time"

const (
    defaultAddSubscriptionTimeout = time.Duration(2) * time.Second
    defaultRemoveSubscriptionTimeout = time.Duration(2) * time.Second
    defaultAddSubscriptionRetry = 5 // 尝试添加subscription最大cas尝试次数
    defaultRemoveSubscriptionRetry = 5 // 尝试删除subscription最大cas尝试次数
    defaultLazyRemoveSubRetry = 5 // 惰性删除每次lookup尝试次数
)

const (
    defaultSnowflakeNode = 22
)
