package eventbus

type Options struct {
	// PoolSize ants 协程池大小；<=0 时使用默认值。
	PoolSize int
	// SnowflakeNode 雪花 Node ID，有效范围 1~1023。
    // 传输的值如果 <= 0, 使用默认值
	SnowflakeNode int64
}

func (o Options) poolSize() int {
	if o.PoolSize <= 0 {
		return defaultGoPoolSize
	}
	return o.PoolSize
}

func (o Options) snowflakeNode() int64 {
	if o.SnowflakeNode <= 0 {
		return defaultSnowflakeNode
	}
	return o.SnowflakeNode
}
