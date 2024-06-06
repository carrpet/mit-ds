package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"slices"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type keyvalues struct {
	key    string
	values []string
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

func MapOutputFilename(taskId, reducerNum int) string {
	return fmt.Sprintf("mr-%d-%d", taskId, reducerNum)
}

func ReduceOutputFilename(reducerNum int) string {
	return fmt.Sprintf("mr-out-%d", reducerNum)
}

func generateReduceInput(numMapTasks, reducerNum int) []string {
	result := make([]string, numMapTasks)
	for i := 0; i < numMapTasks; i++ {
		result[i] = MapOutputFilename(i, reducerNum)
	}

	return result

}

func CheckMapComplete(taskId, numReducers int) bool {
	for i := 0; i < numReducers; i++ {
		if _, err := os.Stat(MapOutputFilename(taskId, i)); errors.Is(err, os.ErrNotExist) {
			return false
		}

	}
	return true
}

func CheckReduceComplete(reducerNum int) bool {

	if _, err := os.Stat(ReduceOutputFilename(reducerNum)); errors.Is(err, os.ErrNotExist) {
		return false
	}

	return true
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
		/*
			if reply.Type != None {
				fmt.Printf("assigned task number %d", reply.TaskNum)
			}
		*/
		if reply.Type == Map {
			// read file contents
			filename := reply.InputFiles[0]
			nReduce := reply.NumReducers
			taskNum := reply.TaskNum
			//os.ReadFile()
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			defer file.Close()

			kvs := mapf(filename, string(content))

			sort.Sort(ByKey(kvs))

			files := make([]*os.File, nReduce)
			filenames := make([]string, nReduce)

			for i := 0; i < nReduce; i++ {
				outFileName := MapOutputFilename(taskNum, i)
				files[i], err = os.CreateTemp("", outFileName)
				filenames[i] = outFileName
				if err != nil {
					log.Fatalf("cannot write intermediate file %v", outFileName)
				}
				defer files[i].Close()
			}

			for _, kv := range kvs {
				reducer := ihash(kv.Key) % nReduce
				files[reducer].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
			}

			if !CheckMapComplete(taskNum, reply.NumReducers) {
				for i, f := range files {
					os.Rename(f.Name(), filenames[i])
				}

			}

		} else if reply.Type == Reduce {
			var files []io.Reader
			for _, fname := range reply.InputFiles {
				file, err := os.Open(fname)
				if err != nil {
					log.Fatalf("Cannot read reduce input file %v", fname)
				}
				files = append(files, file)
			}
			reduced, ok := reduceWork(files)
			if !ok {
				log.Fatalf("reduce work encountered an error")
			}
			results := accumulateResults(reduced)
			outFileName := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNum)
			f, err := os.CreateTemp("", outFileName)
			if err != nil {
				log.Fatalf("cannot open output file for writing: %v", outFileName)
			}
			defer f.Close()

			for _, keyval := range results {
				output := reducef(keyval.key, keyval.values)
				f.WriteString(fmt.Sprintf("%v %v\n", keyval.key, output))
			}
			os.Rename(f.Name(), outFileName)

		} else {
			continue

		}
	}

}

// return the index with the lowest key ignoring ""
func compareLines(lines []string) int {
	valkey := map[string]int{}
	for i, l := range lines {
		keyval := strings.Split(l, " ")
		if len(keyval) == 2 {
			key := keyval[0]
			if key != "" {
				valkey[keyval[0]] = i
			}
		}
	}

	keys := make([]string, len(valkey))
	i := 0
	for k := range valkey {
		keys[i] = k
		i++
	}

	minVal := slices.Min(keys)
	return valkey[minVal]

}

func processNextKey(s *bufio.Scanner) (string, []string, bool) {
	isNewChar := false
	keyval := strings.Split(s.Text(), " ")
	if len(keyval) != 2 {
		return "", nil, false
	}
	vals := []string{}
	thiskey := keyval[0]
	vals = append(vals, keyval[1])
	for s.Scan() {
		keyval := strings.Split(s.Text(), " ")
		if len(keyval) != 2 {
			return "", nil, false
		}
		if keyval[0] != thiskey {
			isNewChar = true
			break
		}
		vals = append(vals, keyval[1])

	}

	return thiskey, vals, !isNewChar

}

func reduceWork(files []io.Reader) ([]keyvalues, bool) {
	results := []keyvalues{}
	scanners := []*bufio.Scanner{}
	for _, s := range files {
		scanners = append(scanners, bufio.NewScanner(s))
	}

	filelines := make([]string, len(scanners))
	numRemaining := len(scanners)

	//initialize filelines
	for i, sc := range scanners {
		if sc.Scan() {
			filelines[i] = sc.Text()
		} else {
			filelines[i] = ""
			numRemaining--
		}

	}

	for numRemaining > 0 {
		nextLineIndex := compareLines(filelines)
		key, values, isEOF := processNextKey(scanners[nextLineIndex])
		if key == "" {
			return nil, false
		}

		results = append(results, keyvalues{key, values})

		if isEOF {
			filelines[nextLineIndex] = ""
			numRemaining--

		} else {
			filelines[nextLineIndex] = scanners[nextLineIndex].Text()
		}

	}

	return results, true

}

func accumulateResults(input []keyvalues) []keyvalues {
	if len(input) == 0 {
		return nil
	}
	i := 0
	accumulator := []string{}
	output := []keyvalues{}
	thisletter := input[0].key
	for i < len(input) {
		for i < len(input) && thisletter == input[i].key {
			accumulator = append(accumulator, input[i].values...)
			i++

		}
		output = append(output, keyvalues{thisletter, accumulator})
		if i < len(input) {
			thisletter = input[i].key
			accumulator = []string{}
		}

	}

	return output

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
	//log.Printf("worker sending http connection request on socket: %s\n", sockname)
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
