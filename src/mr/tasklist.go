package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)

func (t *task) ToReply(nReduce int, r *RequestTaskReply) {
	r.InputFiles = make([]string, len(t.inputFiles))
	copy(r.InputFiles, t.inputFiles)
	r.TaskNum = t.tid.id
	r.NumReducers = nReduce
	r.Type = t.ttype
	r.WorkerId = t.assignedId
	r.ReduceTaskNum = t.tid.reducerId
}

/*
func (t *taskmanager) checkComplete(workerId, taskId int) bool {
	// check the filesystem to see if worker has written the files for all reducers
	for i := 0; i < t.numReducers; i++ {
		if _, err := os.Stat(MapOutputFilename(workerId, taskId, i)); errors.Is(err, os.ErrNotExist) {
			return false
		}

	}
	return true
}
*/

type taskId struct {
	id        int
	reducerId int
}
type task struct {
	ttype      TaskType
	tid        taskId
	status     TaskStatus
	inputFiles []string
	assignedId int
}

type taskmanager struct {
	l                 *sync.Mutex
	assignments       map[int]int
	tasks             []task
	totalCompleted    int
	mapTasksCompleted int
	numMapTasks       int
	numReducers       int
}

func (t *taskmanager) manageTask(task task) {
	// start timer
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		switch task.ttype {
		case Map:
			if CheckMapComplete(task.tid.id, t.numReducers) {
				err := t.completeTask(task.assignedId)
				if err != nil {
					log.Fatalf(err.Error())
				}
				return
			}
		case Reduce:
			if CheckReduceComplete(task.tid.reducerId) {
				err := t.completeTask(task.assignedId)
				if err != nil {
					log.Fatalf(err.Error())
				}
				return
			}

		}

	}
	time.Sleep(1 * time.Second)
	t.unassignTask(task.assignedId)

}

func (t *taskmanager) isDone() bool {
	t.l.Lock()
	defer t.l.Unlock()
	return t.totalCompleted >= len(t.tasks)
}

func (t *taskmanager) isMapPhase() bool { return t.mapTasksCompleted < t.numMapTasks }

// these are non-thread safe helper functions
func (t *taskmanager) findIdleMapTaskId() int {
	for i := 0; i < t.numMapTasks; i++ {
		if t.tasks[i].status == IDLE {
			return i
		}
	}
	return -1

}

func (t *taskmanager) findIdleReduceTaskId() int {
	for i := t.numMapTasks; i < len(t.tasks); i++ {
		if t.tasks[i].status == IDLE {
			return i
		}
	}
	return -1

}

func (t *taskmanager) assignTask(taskId, workerId int) task {
	t.tasks[taskId].status = INPROGRESS
	t.tasks[taskId].assignedId = workerId
	t.assignments[workerId] = taskId
	go t.manageTask(t.tasks[taskId])
	return t.tasks[taskId]

}

// thread safe
func (t *taskmanager) assignNextTask(workerId int) (task, bool) {
	t.l.Lock()
	defer t.l.Unlock()

	// If worker is already assigned a task then
	// don't assign a task
	_, ok := t.assignments[workerId]
	if ok {
		return task{}, false
	}

	var taskId int

	if t.isMapPhase() {
		taskId = t.findIdleMapTaskId()
	} else {
		taskId = t.findIdleReduceTaskId()
	}

	if taskId < 0 {
		return task{}, false
	}

	task := t.assignTask(taskId, workerId)

	return task, true

}

func (t *taskmanager) unassignTask(workerId int) error {
	t.l.Lock()
	defer t.l.Unlock()
	taskId, ok := t.assignments[workerId]
	if !ok {
		return fmt.Errorf("no assignment for worker %d found, unassignTask()", workerId)
	}
	t.tasks[taskId].status = IDLE
	delete(t.assignments, workerId)
	return nil

}

func (t *taskmanager) completeTask(workerId int) error {
	t.l.Lock()
	defer t.l.Unlock()
	taskId, ok := t.assignments[workerId]
	if !ok {
		return fmt.Errorf("no assignment for worker %d found, completeTask()", workerId)
	}
	t.tasks[taskId].status = COMPLETED
	task := t.tasks[taskId]
	if task.ttype == Map {
		t.mapTasksCompleted++
	}
	t.totalCompleted++
	delete(t.assignments, workerId)
	//fmt.Printf("completed task %d\n", task.tid.id)
	return nil

}

func NewTaskManager(input []string, numReducers int) *taskmanager {
	l := &sync.Mutex{}
	numMapTasks := len(input)
	numTasks := numMapTasks + numReducers
	tasks := make([]task, numTasks)
	assignments := map[int]int{}
	for i, fname := range input {
		tasks[i].inputFiles = append(tasks[i].inputFiles, fname)
		tasks[i].status = IDLE
		tasks[i].ttype = Map
		tasks[i].tid.id = i

	}

	reducerId := 0
	// initialize the reduce tasks
	for i := numMapTasks; i < len(tasks); i++ {
		reduceInput := generateReduceInput(numMapTasks, reducerId)
		tasks[i].inputFiles = make([]string, len(reduceInput))
		copy(tasks[i].inputFiles, reduceInput)
		tasks[i].status = IDLE
		tasks[i].ttype = Reduce
		tasks[i].tid.id = i
		tasks[i].tid.reducerId = reducerId
		reducerId++
		//fmt.Printf("reduce input files are: %v", tasks[i].inputFiles)

	}

	return &taskmanager{
		l,
		assignments,
		tasks,
		0,
		0,
		numMapTasks,
		numReducers,
	}

}
