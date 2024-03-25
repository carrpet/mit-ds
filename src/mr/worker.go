package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// put contents in a loop
	for {

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()

		//contents
		reply := RequestTaskReply{}
		if ok := CallRequestTask(&reply); !ok {
			fmt.Println("worker detected error, exiting!")
			return

		}
		if reply.Type == Map {
			// read file contents
			filename := reply.MapParams.InputFile
			nReduce := reply.MapParams.NumReducers
			taskNum := reply.TaskNum
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kvs := mapf(filename, string(content))

			sort.Sort(ByKey(kvs))

			files := make([]*os.File, nReduce)
			filenames := make([]string, nReduce)

			for i := 0; i < nReduce; i++ {
				outFileName := fmt.Sprintf("mr-%d-%d", taskNum, i)
				files[i], err = os.Create(outFileName)
				filenames[i] = outFileName
				if err != nil {
					log.Fatalf("cannot write intermediate file %v", outFileName)
				}
			}

			for _, kv := range kvs {
				reducer := ihash(kv.Key) % nReduce
				files[reducer].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
			}

			for _, f := range files {
				f.Close()
			}

			ok := CallCompleteTask(filenames, taskNum, &CompleteTaskReply{})
			if !ok {
				log.Fatalf("could not confirm task completion for task %d", taskNum)
			}

		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallRequestTask(reply *RequestTaskReply) bool {

	// here we assume that the worker processes are all being run locally
	// in which case the pid is guaranteed to be unique

	//when we port this to run workers on different machines we'll need a different
	// id for uniqueness, maybe IP
	args := RequestTaskArgs{
		ClientId: os.Getpid(),
	}

	return call("Coordinator.RequestTask", &args, reply)

}

func CallCompleteTask(outFiles []string, taskNum int, reply *CompleteTaskReply) bool {
	args := CompleteTaskArgs{
		OutputFiles: outFiles,
		WorkerId:    os.Getpid(),
		TaskNum:     taskNum,
	}

	return call("Coordinator.CompleteTask", &args, reply)
}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	log.Printf("worker sending http connection request on socket: %s\n", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
