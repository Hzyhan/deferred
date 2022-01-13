package queue

import(
	"deferred/tasks"
	"testing"
)

func TestSingleList_Init(t *testing.T)  {
	list := new(SingleList)
	list.Init()
	t.Log("single list init success")
}

func TestSingleList_Append(t *testing.T){
	list := new(SingleList)
	list.Init()

	b := list.Append(nil)
	if b {
		t.Error("single list append nil failed")
	} else {
		t.Log("single list append nil success")
	}
	data := tasks.Signature{Name: "1"}
	b = list.Append(&SingleNode{Data:&data})
	if b {
		t.Log("single list append first success")
	} else {
		t.Error("single list append first failed")
	}
	data2 := tasks.Signature{Name: "2"}
	b = list.Append(&SingleNode{Data:&data2})
	if b {
		t.Log("single list append second success")
	} else {
		t.Error("single list append second failed")
	}
}

func TestSingleList_Insert(t *testing.T){
	list := new(SingleList)
	list.Init()

	b := list.Insert(0, nil)
	if b {
		t.Error("single list insert nil failed")
	} else {
		t.Log("single list insert nil success")
	}
	data := tasks.Signature{Name: "1"}
	b = list.Insert(1, &SingleNode{Data:&data})
	if b {
		t.Error("single list insert out of range failed")
	} else {
		t.Log("single list insert out of range success")
	}
	b = list.Insert(0, &SingleNode{Data:&data})
	if b {
		t.Log("single list insert first success")
	} else {
		t.Error("single list insert first failed")
	}
	data2 := tasks.Signature{Name: "2"}
	b = list.Insert(1, &SingleNode{Data:&data2})
	data3 := tasks.Signature{Name: "2"}
	b = list.Insert(2, &SingleNode{Data:&data3})
	if b {
		t.Log("single list insert multi success")
	} else {
		t.Error("single list insert multi failed")
	}
}

func TestSingleList_Delete(t *testing.T){
	list := new(SingleList)
	list.Init()

	b := list.Delete(0)
	if b {
		t.Error("single list delete out of range failed")
	} else {
		t.Log("single list delete out of range success")
	}
	data := tasks.Signature{Name: "1"}
	list.Append(&SingleNode{Data:&data})

	b = list.Delete(0)
	if b {
		t.Log("single list delete first success")
	} else {
		t.Error("single list delete first failed")
	}
	data2 := tasks.Signature{Name: "2"}
	data3 := tasks.Signature{Name: "3"}
	list.Append(&SingleNode{Data:&data})
	list.Append(&SingleNode{Data:&data2})
	list.Append(&SingleNode{Data:&data3})

	b = list.Delete(2)
	if b {
		t.Log("single list delete third success")
	} else {
		t.Error("single list delete third failed")
	}
}
