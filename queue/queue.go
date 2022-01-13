package queue

import (
	"deferred/tasks"
	"errors"
)

// Queue 队列信息
type Queue struct{
	list *SingleList
	Name string
}

// Init 队列初始化
func (q *Queue) Init()  {
	q.list = new(SingleList)
	q.list.Init()
	IQueueMap.LoadOrStore(q.Name, q)
}

// Size 获取队列长度
func (q *Queue) Size() uint{
	return q.list.Size
}

// Enqueue 进入队列
func (q *Queue) Enqueue(data *tasks.Signature) error {
	ok := q.list.Append(&SingleNode{Data:data})
	if !ok {
		return errors.New("Enqueue err")
	}
    return nil
}

// Dequeue 出列
func (q *Queue) Dequeue() *tasks.Signature {
	node := q.list.Get(0)
	if node == nil{
		return nil
	}
	q.list.Delete(0)
	return node.Data
}

// Peek 查看队头信息
func (q *Queue)Peek() *tasks.Signature {
	node := q.list.Get(0)
	if node == nil{
		return nil
	}
	return node.Data
}

func (q *Queue)Close() error {
	return nil
}