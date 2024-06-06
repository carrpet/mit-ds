package mr

import (
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

type Coordinator struct {
	// Your definitions here.
	taskmanager
}

func NewCoordinator(input []string, numReducers int) Coordinator {
	// the number of tasks is the number of input files plus the nReduce param
	//tasks := make([]Task, 0, len(files)+nReduce)
	taskmanager := *NewTaskManager(input, numReducers)
	return Coordinator{
		taskmanager,
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
	task, ok := c.assignNextTask(req.ClientId)
	if !ok {
		reply.Type = None
		return nil
	}
	//fmt.Printf("Task type is: %v, inputfiles is: %v", task.ttype, task.inputFiles)

	task.ToReply(c.numReducers, reply)

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

	//log.Printf("coordinator listening on socket %s\n", sockname)

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
	return &c
}
