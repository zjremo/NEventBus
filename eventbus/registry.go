package eventbus

import (
	"maps"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

func copySubMap(subMap map[SubscriptionID]struct{}) map[SubscriptionID]struct{} {
    newSubMap := make(map[SubscriptionID]struct{}, len(subMap))
    for subID := range subMap {
        newSubMap[subID] = struct{}{}
    }
    return newSubMap
}

// subscriptionTable 注册表
type subscriptionTable struct {
    topics map[Topic]map[SubscriptionID]struct{} // topic -> subs
    subIds map[SubscriptionID]*Subscription // subID -> sub
}

// Registry 持有注册表的引用
type Registry struct {
    table atomic.Pointer[subscriptionTable]
}

// NewRegistry 获取一个新的注册仓库
func NewRegistry() *Registry {
    table := &subscriptionTable{
        topics: make(map[Topic]map[SubscriptionID]struct{}),
    }

    registry := &Registry{}
    registry.table.Store(table)

    return registry
}

/*
    Lookup 获取topic下的所有订阅触发器
    1. lookup the subsciption in topic;
    2. lazy delete the subscription been deleted
*/
func (r *Registry) Lookup(topic Topic) []*Subscription {
    table := r.table.Load()

    submap, ok := table.topics[topic]
    if !ok {
        return nil
    }

    subscriptions := make([]*Subscription, 0)
    for subID := range submap {
        if _, ok := table.subIds[subID]; !ok {
            delete(submap, subID)
            continue
        }
        subscriptions = append(subscriptions, table.subIds[subID])
    }
    return subscriptions
}

/* Cow机制，克隆table写入，然后替换引用 */
// cloneTableWithTopic 仅仅深拷贝要修改Topic下注册触发器，其他一律浅拷贝
// 如果Topic是新的不存在的，那么全量浅拷贝
func (r *Registry) cloneTableWithTopic(old *subscriptionTable, topic Topic) *subscriptionTable {
    newTopics := make(map[Topic]map[SubscriptionID]struct{}, len(old.topics))
    newSubIds := make(map[SubscriptionID]*Subscription, len(old.subIds))

    // Step 1: copy Topics to newTopics
    for t, subMap := range old.topics {
        // 1.1. topic不是新的，此时需要深拷贝topic下的订阅回调 
        if topic == t {
            newSubMap := copySubMap(subMap)
            newTopics[topic] = newSubMap
            continue
        }

        // 1.2. topic是新的，此时直接浅拷贝
        newTopics[t] = subMap 
    }

    // Step2: copy SubIds to newSubIds
    maps.Copy(newSubIds, old.subIds)

    // Step3: return new subscriptionTable
    return &subscriptionTable{
        topics: newTopics,
        subIds: newSubIds,
    }
}

// AddSubscription 添加注册回调到注册表中
func (r *Registry) AddSubscription(topic Topic, sub *Subscription, timeout time.Duration) error {
    if timeout <=0 {
        timeout = defaultAddSubscriptionTimeout
    }
    timer := time.NewTimer(timeout)

    for {
        select {
        case <-timer.C:
            return errors.New("AddSubscription timeout!")
        default:
            oldTable := r.table.Load()
            newTable := r.cloneTableWithTopic(oldTable, topic)
            // copy-on-write, begin modify operation
            // 1. modify topics
            _, ok := newTable.topics[topic]
            if !ok {
                newTable.topics[topic] = make(map[SubscriptionID]struct{})
            }
            subMap := newTable.topics[topic]
            subMap[sub.id] = struct{}{}

            // 2. modify subIds
            newTable.subIds[sub.id] = sub

            // 3. modify reference
            if r.table.CompareAndSwap(oldTable, newTable){
                return nil
            }
        }
    }
}

func (r *Registry) RemoveSubscription(subID SubscriptionID) {
    table := r.table.Load()

    // 采取惰性删除的方式，只删除subIds中的subID，topics中的分摊到后续的lookup中 
    delete(table.subIds, subID)
}
