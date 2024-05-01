package mr

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	id     int
	worker int
	status TaskStatus
	input  string
}

func (t *mapTask) isComplete(numReducers int) bool {
	// check the filesystem to see if worker has written the files for all reducers
	for i := 0; i < numReducers; i++ {
		if _, err := os.Stat(MapOutputFilename(t.worker, t.id, i)); errors.Is(err, os.ErrNotExist) {
			return false
		}

	}
	return true
}

type reduceTask struct {
	id         int
	status     TaskStatus
	inputFiles []string
}
type workload interface {
	assignMapTask(wid int) (mapTask, bool)
	assignReduceTask(wid int) (reduceTask, bool)
	isMapPhase() bool
	reconcile()
	isDone() bool
}

type tasks struct {
	l              *sync.Mutex
	cancelCh       chan int
	mapTasks       []mapTask
	mapAssignments map[int]int
	reduceTasks    []reduceTask
	mapPhase       bool
	numReducers    int
	numCompleted   int
}

func cancelCallback(ch chan int, workerId int) {
	// start timer
	time.Sleep(10 * time.Second)
	ch <- workerId

}

func (t *tasks) isDone() bool {
	return t.numCompleted >= len(t.mapTasks)
}
func (t *tasks) getAssignedMapTask(workerId int) (*mapTask, bool) {
	tid, ok := t.mapAssignments[workerId]
	if !ok {
		return nil, false
	}
	return &t.mapTasks[tid], true
}

func (t *tasks) reconcile() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case workerId := <-t.cancelCh:
			t.unassignMapTask(workerId)
		}
	}

}

func (t *tasks) unassignMapTask(workerId int) {
	t.l.Lock()
	defer t.l.Unlock()
	task, ok := t.getAssignedMapTask(workerId)
	if !ok {
		println("no assignment for worker found, reconcile()")
		return
	}
	// first check if the task is complete
	if task.isComplete(t.numReducers) {
		task.status = COMPLETED
		delete(t.mapAssignments, workerId)
		t.numCompleted++
		return
	}
	task.status = IDLE
	delete(t.mapAssignments, workerId)

}

func (t *tasks) assignMapTask(wid int) (mapTask, bool) {
	t.l.Lock()
	defer t.l.Unlock()
	val, ok := t.mapAssignments[wid]
	if ok {
		return t.mapTasks[val], true
	}
	for i := range t.mapTasks {
		if t.mapTasks[i].status == IDLE {
			t.mapTasks[i].status = INPROGRESS
			t.mapTasks[i].worker = wid
			t.mapAssignments[wid] = i
			go cancelCallback(t.cancelCh, wid)
			return t.mapTasks[i], true
		}
	}

	return mapTask{}, false
}

func (t *tasks) assignReduceTask(wid int) (reduceTask, bool) {
	return reduceTask{}, false
}

func (t *tasks) isMapPhase() bool {
	return true
}

type tasksFactory struct {
	input       []string
	numReducers int
}

func (tf *tasksFactory) createWorkload() workload {
	l := &sync.Mutex{}
	cancelCh := make(chan int)
	mapTasks := make([]mapTask, 0, len(tf.input))
	reduceTasks := make([]reduceTask, 0, tf.numReducers)
	mapAssignments := map[int]int{}
	numReducers := tf.numReducers
	inputs := tf.input
	for i, input := range inputs {
		mapTasks = append(mapTasks, mapTask{
			id:     i,
			status: IDLE,
			input:  input,
		})

	}

	return &tasks{
		l,
		cancelCh,
		mapTasks,
		mapAssignments,
		reduceTasks,
		true,
		numReducers,
		0,
	}

}
