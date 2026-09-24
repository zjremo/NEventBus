package eventbus

import "github.com/pkg/errors"

var (
	ErrNilHandler           = errors.New("eventbus: handler is nil!")
	ErrEmptyTopic           = errors.New("eventbus: empty topic!")
	ErrNilEvent             = errors.New("eventbus: event is nil!")
	ErrTopicNotFound        = errors.New("Topic is not found!")
	ErrSubIDNotFound        = errors.New("SubscriptionID not found!")
	ErrSubExecutionFailed   = errors.New("eventbus: subscription callback failed!")
	ErrAddSubWaitTimeout    = errors.New("Add subscription wait timeout!")
	ErrRemoveSubWaitTimeout = errors.New("Remove subscription wait timeout!")
	ErrExceedMaxRetry       = errors.New("exceeded maximum number of retries!")
)
