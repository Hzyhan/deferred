package queue

import(
	"deferred/tasks"
	"testing"
)

func TestQueue_Init(t *testing.T)  {
	q := new(Queue)
	q.Init()
}

func TestQueue_Enqueue(t *testing.T){
	q := new(Queue)
	q.Init()
	data := tasks.Signature{Name: "1"}
	q.Enqueue(&data)
	if 1 == q.Size(){
		t.Log("queue size and enquqeuesuccess")
	} else {
		t.Error("queue size and enqueue failed")
	}
}

func TestQueue_Dequeue(t *testing.T){
	q := new(Queue)
	q.Init()

	d1 := q.Dequeue()
	if d1 == nil{
		t.Log("empty queue dequeue success")
	} else {
		t.Error("empty queue dequeue failed")
	}
	data := tasks.Signature{Name: "1"}
	q.Enqueue(&data)
	d := q.Dequeue()
	if "1" == d.Name && q.Size() == 0{
		t.Log("queue dequeue success")
	} else {
		t.Error("queue dequeue failed")
	}
}

func TestQueue_Peek(t *testing.T){
	q := new(Queue)
	q.Init()

	d1 := q.Peek()
	if d1 == nil{
		t.Log("empty queue peek success")
	}else {
		t.Error("empty queue peek failed")
	}
	data := tasks.Signature{Name: "1"}
	q.Enqueue(&data)
	d := q.Peek()
	if "1" == d.Name && q.Size() == 1{
		t.Log("queue peek success")
	} else {
		t.Error("queue peek failed")
	} 
}
