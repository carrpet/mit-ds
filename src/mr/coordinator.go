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

func (t mapTask) ToReply(nReduce int, r *RequestTaskReply) {
	r.MapParams.InputFile = t.input
	r.TaskNum = t.id
	r.MapParams.NumReducers = nReduce
	r.Type = Map
	r.WorkerId = t.worker
}

type Coordinator struct {
	// Your definitions here.
	numReducers int
	//isDone      bool
	//tasklist
	workload
}

func NewCoordinator(input []string, numReducers int) Coordinator {
	// the number of tasks is the number of input files plus the nReduce param
	//tasks := make([]Task, 0, len(files)+nReduce)
	tasksFactory := &tasksFactory{
		input,
		numReducers,
	}
	workload := tasksFactory.createWorkload()
	return Coordinator{
		numReducers,
		//isDone:      false,
		workload,
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

	if c.isMapPhase() {

		assigned, ok := c.assignMapTask(req.ClientId)
		if !ok {
			return errors.New("could not find a task to assign")

		}
		assigned.ToReply(c.numReducers, reply)
		return nil
	}

	return nil

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

	return c.isDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)

	// Your code here.
	c.server()
	go c.reconcile()
	return &c
}
