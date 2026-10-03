package eventbus

import (
	"maps"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type subSet map[SubscriptionID]*Subscription

type topicEntry struct {
	subs atomic.Pointer[subSet]
}

type Registry struct {
	topicMapMutex sync.RWMutex
	topicMap      map[Topic]struct{}
	buckets       map[Topic]*topicEntry
	byID          sync.Map // SubscriptionID -> *Subscription
}

func NewRegistry() *Registry {
	return &Registry{
		topicMap: make(map[Topic]struct{}),
		buckets:  make(map[Topic]*topicEntry),
	}
}

func (r *Registry) createTopic(topic Topic) {
	r.topicMapMutex.Lock()
	defer r.topicMapMutex.Unlock()

    // 从topicMap检查当前topic是否真实存在
	if _, live := r.topicMap[topic]; live {
		return
	}

	entry, ok := r.buckets[topic]
	if !ok {
		entry = &topicEntry{}
		r.buckets[topic] = entry
	}

	subs := make(subSet)
	entry.subs.Store(&subs)
	r.topicMap[topic] = struct{}{}
}

func (r *Registry) removeTopic(topic Topic) {
	r.topicMapMutex.Lock()
	defer r.topicMapMutex.Unlock()
	delete(r.topicMap, topic)
}

func (r *Registry) hasTopic(topic Topic) bool {
	r.topicMapMutex.RLock()
	defer r.topicMapMutex.RUnlock()
	_, ok := r.topicMap[topic]
	return ok
}

func (r *Registry) bucket(topic Topic) *topicEntry {
	r.topicMapMutex.RLock()
	defer r.topicMapMutex.RUnlock()
	return r.buckets[topic]
}

func (r *Registry) ListAllTopics() []Topic {
	r.topicMapMutex.RLock()
	defer r.topicMapMutex.RUnlock()

	copyTopics := make([]Topic, 0, len(r.topicMap))
	for topic := range r.topicMap {
		copyTopics = append(copyTopics, topic)
	}
	return copyTopics
}

func (r *Registry) Lookup(topic Topic) []*Subscription {
	if !r.hasTopic(topic) {
		return nil
	}
	entry := r.bucket(topic)
	if entry == nil {
		return nil
	}
	setp := entry.subs.Load()
	if setp == nil {
		return nil
	}
	set := *setp
	if len(set) == 0 {
		return nil
	}
	out := make([]*Subscription, 0, len(set))
	for _, sub := range set {
		out = append(out, sub)
	}
	return out
}

func (r *Registry) AddSubscription(topic Topic, sub *Subscription, waitTime time.Duration) error {
	if waitTime <= 0 {
		waitTime = defaultAddSubscriptionTimeout
	}
	if !r.hasTopic(topic) {
		return ErrTopicNotFound
	}
	entry := r.bucket(topic)
	if entry == nil {
		return ErrTopicNotFound
	}

	sub.topic = topic
	deadline := time.Now().Add(waitTime)
	for {
		if !r.hasTopic(topic) {
			return ErrTopicNotFound
		}
		old := entry.subs.Load()
		next := cloneSubSet(old)
		next[sub.id] = sub
		if entry.subs.CompareAndSwap(old, &next) {
			r.byID.Store(sub.id, sub)
			return nil
		}
		if time.Now().After(deadline) {
			return ErrAddSubWaitTimeout
		}
		runtime.Gosched()
	}
}

func (r *Registry) RemoveSubscription(subID SubscriptionID, waitTime time.Duration) error {
	if waitTime <= 0 {
		waitTime = defaultRemoveSubscriptionTimeout
	}

	v, ok := r.byID.Load(subID)
	if !ok {
		return ErrSubIDNotFound
	}
	sub := v.(*Subscription)

	r.topicMapMutex.Lock()
	_, live := r.topicMap[sub.topic]
	entry, hasBucket := r.buckets[sub.topic]
	if !live {
		// RemoveTopic 已摘掉 topicMap：buckets 里若还有残留，第一次退订时惰删；
		// 后续退订 buckets 已无该 topic，只清 byID 即可。
		if hasBucket {
			delete(r.buckets, sub.topic)
		}
		r.topicMapMutex.Unlock()
		r.byID.Delete(subID)
		return nil
	}
	r.topicMapMutex.Unlock()

	if entry == nil {
		r.byID.Delete(subID)
		return nil
	}

	deadline := time.Now().Add(waitTime)
	for {
		old := entry.subs.Load()
		if old != nil {
			if _, exists := (*old)[subID]; !exists {
				r.byID.Delete(subID)
				return nil
			}
		}
		next := cloneSubSet(old)
		delete(next, subID)
		if entry.subs.CompareAndSwap(old, &next) {
			r.byID.Delete(subID)
			return nil
		}
		if time.Now().After(deadline) {
			return ErrRemoveSubWaitTimeout
		}
		runtime.Gosched()
	}
}

func (r *Registry) release() {
	r.topicMapMutex.RLock()
	entries := make([]*topicEntry, 0, len(r.buckets))
	for _, e := range r.buckets {
		entries = append(entries, e)
	}
	r.topicMapMutex.RUnlock()

	for _, e := range entries {
		setp := e.subs.Load()
		if setp == nil {
			continue
		}
		for _, sub := range *setp {
			sub.release()
		}
	}
}

func cloneSubSet(old *subSet) subSet {
	if old == nil || len(*old) == 0 {
		return make(subSet, 1)
	}
	next := make(subSet, len(*old)+1)
	maps.Copy(next, *old)
	return next
}
