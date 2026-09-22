package eventbus

import "time"

const (
    epochDeleteMaxCount = 20 // 每次查询subscription 最多惰性删除topics中的subID数
    defaultAddSubscriptionTimeout = time.Duration(1) * time.Second
)
