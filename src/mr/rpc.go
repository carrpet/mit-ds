package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
	ClientId int
}

type CompleteTaskArgs struct {
	OutputFiles []string
	WorkerId    int
	TaskNum     int
}

type CompleteTaskReply struct{}

type MapWorkerParams struct {
	InputFile   string
	NumReducers int
}

type ReduceWorkerParams struct {
}

type RequestTaskReply struct {
	Type      TaskType
	TaskNum   int
	MapParams MapWorkerParams
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
