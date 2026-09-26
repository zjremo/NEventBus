package eventbus

type executor interface {
    executeTask(subs []*Subscription) map[SubscriptionID]*Result
}
