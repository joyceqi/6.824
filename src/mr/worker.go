package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
	intermediate []KeyValue
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// make worker
	worker := WorkerInfo{}
	worker.mapf = mapf
	worker.reducef = reducef

	for true {
		mapTask := FetchMapTask()
		if len(mapTask.files) == 0 {
			// all map tasks have been allocated
			break
		}
		ProcessMapTask(mapTask.files, &worker)
		WriteToMediate(mapTask.id, mapTask.nReduce, &worker)

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// ask the master for a task
func FetchMapTask() Task {
	args := WorkerHost{}
	args.ip = "127.0.0.1"
	args.port = 1234

	reply := Task{}

	call("Master.AllocateMapTask", &args, &reply)
	return reply
}

// process task with one file
func ProcessMapTask(files []string, worker *WorkerInfo) {
	worker.intermediate = []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := worker.mapf(filename, string(content))
		worker.intermediate = append(worker.intermediate, kva...)
	}
}

// write map task's intermediate data to temporary file and then rename to mr-x-y
// x: map task id
// y: hash(key) % nReduce
func WriteToMediate(mapTaskId int, nReduce int, worker *WorkerInfo) {
	reduceKVMap := make(map[int][]KeyValue)
	for _, kva := range worker.intermediate {
		k := ihash(kva.Key) % nReduce
		reduceKVMap[k] = append(reduceKVMap[k], kva)
	}
	mediateFilenameMap := make(map[string]string)
	for y, kvs := range(reduceKVMap) {
		filename := fmt.Sprint("mr-%d-%d", mapTaskId, y)
		data := ""
		for i, kv := range kvs {
			data += fmt.Sprint("%s %s", kv.Key, kv.Value)
			if i < len(kvs)-1 {
				data += "\n"
			}
		}
		err := ioutil.WriteFile(filename, []byte(data), 0644)
		if err != nil {
			log.Fatalf("cannot write %v", filename)
		} else {
			mediateFilenameMap[filename] = ""
		}
	}
	mediateFilenames := []string{}
	for filename, _ := range(mediateFilenameMap) {
		mediateFilenames = append(mediateFilenames, filename)
	}
	var reply int
	call("Master.ReportMediateFile", mediateFilenames, reply)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
