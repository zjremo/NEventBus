package eventbus

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

// topicMapMutex 创建topic的频率很低，基本不存在并发问题，直接利用读写锁即可
var topicMapMutex sync.RWMutex

// subscriptionTable 注册表
type subscriptionTable struct {
	topics map[Topic]map[SubscriptionID]struct{} // topic -> subs
	subIds map[SubscriptionID]*Subscription      // subID -> sub
}

// Registry 持有注册表的引用
type Registry struct {
	table atomic.Pointer[subscriptionTable]
    topicMap map[Topic]*TopicConfig
}

// NewRegistry 获取一个新的注册仓库
func NewRegistry() *Registry {
	table := &subscriptionTable{
		topics: make(map[Topic]map[SubscriptionID]struct{}),
		subIds: make(map[SubscriptionID]*Subscription),
	}

    registry := &Registry{
        topicMap: make(map[Topic]*TopicConfig),
    }
	registry.table.Store(table)

	return registry
}

// createTopic 创建Topic
func (r *Registry) createTopic(topic Topic, concurMode ConcurrencyMode) {
    topicMapMutex.Lock()
    defer topicMapMutex.Unlock()

    r.topicMap[topic] = &TopicConfig{
        Mode: concurMode,
    }
}

// removeTopic 只删除topicMap的topic键，topics中的惰性删除
func (r *Registry) removeTopic(topic Topic) {
    topicMapMutex.Lock()
    defer topicMapMutex.Unlock()

    delete(r.topicMap, topic)
}

func (r *Registry) hasTopic(topic Topic) bool {
    topicMapMutex.RLock()
    defer topicMapMutex.RUnlock()

    _, ok := r.topicMap[topic]
    return ok
}

func (r *Registry) getTopicConfig(topic Topic) *TopicConfig {
    topicMapMutex.RLock()
    defer topicMapMutex.RUnlock()

    return r.topicMap[topic]
}

func (r *Registry) ListAllTopics() map[Topic]*TopicConfig {
    topicMapMutex.RLock()
    defer topicMapMutex.RUnlock()

    copyMap := make(map[Topic]*TopicConfig, len(r.topicMap)) 
    for topic, config := range r.topicMap {
        nconfig := &TopicConfig{
            Mode: config.Mode,
        }
        copyMap[topic] = nconfig
    }
    return copyMap
}

/*
Lookup 获取topic下的所有订阅触发器
惰删topics中的subId与topic，如果此次没有完成，就交给下次完成。不以牺牲性能为代价来完成删除
*/
func (r *Registry) Lookup(topic Topic) (subscriptions []*Subscription) {
	for range defaultLazyRemoveSubRetry {
		oldTable := r.table.Load()

		submap, ok := oldTable.topics[topic]
		if !ok {
			return nil
		}

        // 查询TopicMap来获取是否真实存在topic
        hasTopic := r.hasTopic(topic)
        var newSubmap map[SubscriptionID]struct{}

        if hasTopic {
            subscriptions = make([]*Subscription, 0, len(submap))
            newSubmap = make(map[SubscriptionID]struct{}, len(submap))

            stale := false
            for subID := range submap {
                sub, ok := oldTable.subIds[subID]
                if !ok {
                    stale = true
                    continue
                }

                subscriptions = append(subscriptions, sub)
                newSubmap[subID] = struct{}{}
            }

            if !stale { // 没有要lazy delete的
                return
            }
        }

		// 此时执行lazy delete操作
		newTopics := make(map[Topic]map[SubscriptionID]struct{}, len(oldTable.topics))
		maps.Copy(newTopics, oldTable.topics)

        if hasTopic {
		    newTopics[topic] = newSubmap
        } else {
            delete(newTopics, topic)
        }

		newTable := &subscriptionTable{
			topics: newTopics,
			subIds: oldTable.subIds,
		}

		if r.table.CompareAndSwap(oldTable, newTable) {
			return
		}
	}
	return
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
func (r *Registry) AddSubscription(topic Topic, sub *Subscription, waitTime time.Duration) error {
	if waitTime <= 0 {
		waitTime = defaultAddSubscriptionTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	for range defaultAddSubscriptionRetry {
		select {
		case <-timer.C:
			return ErrAddSubWaitTimeout
		default:
			oldTable := r.table.Load()
			newTable := r.cloneTableWithTopic(oldTable, topic)
			// copy-on-write, begin modify operation
			// 1. modify topics
			subMap, ok := newTable.topics[topic]
			if !ok || !r.hasTopic(topic) {
                return ErrTopicNotFound 
			}
			subMap[sub.id] = struct{}{}

			// 2. modify subIds
			newTable.subIds[sub.id] = sub

			// 3. modify reference
			if r.table.CompareAndSwap(oldTable, newTable) {
				return nil
			}
		}
	}
	return errors.Wrapf(ErrExceedMaxRetry, "Add subscription, subscription: %#v", sub)
}

func (r *Registry) RemoveSubscription(subID SubscriptionID, waitTime time.Duration) error {
	if waitTime <= 0 {
		waitTime = defaultRemoveSubscriptionTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	for range defaultRemoveSubscriptionRetry {
		select {
		case <-timer.C:
			return ErrRemoveSubWaitTimeout
		default:
			oldTable := r.table.Load()

			if _, ok := oldTable.subIds[subID]; !ok {
				return ErrSubIDNotFound
			}

			newSubIds := make(map[SubscriptionID]*Subscription, len(oldTable.subIds)-1)
			for id, sub := range oldTable.subIds {
				if id != subID {
					newSubIds[id] = sub
				}
			}
			newTable := &subscriptionTable{
				topics: oldTable.topics,
				subIds: newSubIds,
			}

			if r.table.CompareAndSwap(oldTable, newTable) {
				return nil
			}
		}
	}

	return errors.Wrapf(ErrExceedMaxRetry, "Remove subscription, subscriptionID: %s", subID)
}

func copySubMap(subMap map[SubscriptionID]struct{}) map[SubscriptionID]struct{} {
	newSubMap := make(map[SubscriptionID]struct{}, len(subMap))
	for subID := range subMap {
		newSubMap[subID] = struct{}{}
	}
	return newSubMap
}
