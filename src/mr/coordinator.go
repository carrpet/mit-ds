package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	INPROGRESS
	COMPLETED
)

type Task struct {
	Num        int
	Status     TaskStatus
	Worker     int
	InputFiles []string
	Type       TaskType
}

func (t Task) ToReply(nReduce int, r *RequestTaskReply) {
	r.MapParams.InputFile = t.InputFiles[0]
	r.TaskNum = t.Num
	r.MapParams.NumReducers = nReduce
	r.Type = t.Type
}

type TaskList struct {
	tasks       []Task
	assignments map[int]*Task
}

func NewTaskList(inFiles []string) TaskList {

	initTasks := make([]Task, len(inFiles))

	for i, name := range inFiles {
		initTasks[i] = Task{
			Num:        i,
			Status:     IDLE,
			InputFiles: []string{name},
		}

	}

	return TaskList{
		tasks:       initTasks,
		assignments: map[int]*Task{},
	}

}

func (t TaskList) GetAssignedTask(id int) (*Task, bool) {
	val, exists := t.assignments[id]
	return val, exists

}

func (l TaskList) AssignNewTask(workerId int) (*Task, bool) {
	for _, t := range l.tasks {
		if t.Status == IDLE {
			t.Status = INPROGRESS
			t.Worker = workerId
			l.assignments[workerId] = &t
			return &t, true
		}
	}

	return nil, false

}

type Coordinator struct {
	// Your definitions here.
	TaskList    TaskList
	NumReducers int
	isDone      bool
}

func New(files []string, nReduce int) Coordinator {
	// the number of tasks is the number of input files plus the nReduce param
	//tasks := make([]Task, 0, len(files)+nReduce)

	return Coordinator{
		TaskList:    NewTaskList(files),
		NumReducers: nReduce,
		isDone:      false,
	}
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(req *RequestTaskArgs, reply *RequestTaskReply) error {
	// if client has already been assigned a task just return it, since it may be the case
	// that the client died and restarted and now needs that task reassigned to it
	assigned, exists := c.TaskList.GetAssignedTask(req.ClientId)
	if exists {
		assigned.ToReply(c.NumReducers, reply)
		return nil

	}

	assigned, ok := c.TaskList.AssignNewTask(req.ClientId)
	if !ok {
		return errors.New("could not find a task to assign")

	}
	assigned.ToReply(c.NumReducers, reply)
	return nil

}

func (c *Coordinator) CompleteTask(req *CompleteTaskArgs, reply *CompleteTaskReply) error {

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Printf("coordinator listening on socket %s\n", sockname)

	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := New(files, nReduce)

	// Your code here.

	c.server()
	return &c
}
