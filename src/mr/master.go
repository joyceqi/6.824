package mr

import (
	"log"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Master struct {
	// Your definitions here.
	mapList []Task
	reduceList []Task
	nReduce int
	mapAllocated bool
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// allocate map task to each worker
func (m *Master) AllocateMapTask(args *WorkerHost, reply *Task) error {
	m.lock.Lock()
	var allFinished bool = true
	if m.mapAllocated == false {
		for _, mapTask := range m.mapList {
			curTime := time.Now().Unix()
			if mapTask.status != "FINISHED" {
				allFinished = false
			}
			if mapTask.status == "NOTASSIGNED" ||
				(mapTask.status == "ASSIGNED" && mapTask.allocateTime > 0 && curTime-mapTask.allocateTime > 10) {
				mapTask.status = "ASSIGNED"
				mapTask.allocateTime = curTime
				reply.id = mapTask.id
				reply.stage = mapTask.stage
				reply.files = mapTask.files
				reply.nReduce = mapTask.nReduce
				return nil
			}
		}
	}
	if allFinished {
		m.mapAllocated = true
		//AllocateReduceTask()
	} else {
		// not all "FINISHED", but some "ASSIGNED" with execution time < 10s
		reply.stage = "WAIT"
		return nil
	}
	m.lock.Unlock()
	reply.files = []string{}
	return nil
}

// record mediate filename in master.reduceList
func (m *Master) ReportMediateFile(args []string, reply int) error {
	m.lock.Lock()
	var mapTaskid int
	for _, filename := range args {
		ret := strings.Split(filename, "-")
		mapTaskid, _ = strconv.Atoi(ret[1])
		y, _ := strconv.Atoi(ret[2])
		m.reduceList[y].files = append(m.reduceList[y].files, filename)
	}
	m.mapList[mapTaskid].status = "FINISHED"
	m.lock.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// init master
	for i, file := range files {
		mt := Task{}
		mt.id = i
		mt.stage = "MAP"
		mt.files= []string {file}
		mt.status = "NOTASSIGNED"
		mt.nReduce = nReduce
		m.mapList = append(m.mapList, mt)
	}
	m.nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		rt := Task{}
		rt.id = i
		rt.stage = "REDUCE"
		rt.status = "NOTASSIGNED"
		rt.nReduce = nReduce
		m.reduceList = append(m.reduceList, rt)
	}
	m.mapAllocated = false

	m.server()
	return &m
}
