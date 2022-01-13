package queue

import (
	"deferred/tasks"
	"sync"
)

// IQueueMap 队列信息map[string]*IQueue{}
var IQueueMap = sync.Map{}

// IQueue 队列
type IQueue interface {
	Init()
	Enqueue(data *tasks.Signature) error
	Dequeue() *tasks.Signature
	Peek() *tasks.Signature
	Close() error
}
