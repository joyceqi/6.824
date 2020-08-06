package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
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

	for {
		flag := RunTaskCall(mapf, reducef)
		time.Sleep(time.Second)
		if flag == 0 {
			break
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func RunTaskCall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) int {

	args := RequestTaskArgs{}
	reply := Task{}
	call("Master.AskForTask", &args, &reply)

	if reply.stage == "MAP" {
		fmt.Println("Worker start a map task")
		return RunMapTask(reply, mapf)
	} else if reply.stage == "REDUCE" {
		fmt.Println("Worker start a reduce task")
		return RunReduceTask(reply, reducef)
	} else if reply.stage == "WAIT" {
		fmt.Println("Worker wait for task")
		return 1
	} else if reply.stage == "EXIT" {
		fmt.Println("No tasks worker exit")
		return 0
	}
	return 1
}

func RunMapTask(mapTask Task, mapf func(string, string) []KeyValue) int {
	files := mapTask.files
	nReduce := mapTask.nReduce
	mapTaskId := mapTask.id

	// apply mapf() on mapTask.files to get []KeyValue
	reduceKVMap := make(map[int][]KeyValue)
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
		kvs := mapf(filename, string(content))
		for _, kv := range kvs {
			k := ihash(kv.Key) % nReduce
			reduceKVMap[k] = append(reduceKVMap[k], kv)
		}
	}

	// write map result into nReduce intermediate files: mr-x-y
	// x: map task id
	// y: hash(key) % nReduce
	var mediateFilenames []string
	for y := 0; y < nReduce; y++ {
		if kvs, ok := reduceKVMap[y]; ok {
			filename := fmt.Sprintf("mr-%d-%d", mapTaskId, y)
			data := ""
			for i, kv := range kvs {
				data += fmt.Sprintf("%s %s", kv.Key, kv.Value)
				if i < len(kvs)-1 {
					data += "\n"
				}
			}
			err := ioutil.WriteFile(filename, []byte(data), 0666)
			if err != nil {
				log.Fatalf("cannot write %v", filename)
			} else {
				mediateFilenames = append(mediateFilenames, filename)
			}
		}
	}
	fmt.Println("Finish a MAP task")

	rAckargs := RequestAckArgs{}
	rAckargs.taskType = "MAP"
	rAckargs.files = mediateFilenames
	replyAckargs := ReplyAckArgs{}
	call("Master.ConfirmState", &rAckargs, &replyAckargs)

	return 1
}

func RunReduceTask(mapTask Task, reducef func(string, []string) string) int {
	return 1
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
