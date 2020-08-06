package mr

import (
	"fmt"
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
	isMapFinished bool
	isReduceFinished bool
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// worker ask master for a task
func (m *Master) AskForTask(args *RequestTaskArgs, reply *Task) error {
	fmt.Println("Start AskForTask")
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isReduceFinished == true {
		reply.stage = "EXIT"
		return nil
	}

	if m.isMapFinished == false {
		var allFinished bool = true
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
		if allFinished == false {
			// not all "FINISHED", but some "ASSIGNED" with execution time < 10s
			reply.stage = "WAIT"
			return nil
		}
	} else if m.isReduceFinished == false {
		for _, reduceTask := range m.reduceList {
			curTime := time.Now().Unix()
			if reduceTask.status == "NOTASSIGNED" ||
				(reduceTask.status == "ASSIGNED" && reduceTask.allocateTime > 0 && curTime-reduceTask.allocateTime > 10) {
				reduceTask.status = "ASSIGNED"
				reduceTask.allocateTime = curTime
				reply.id = reduceTask.id
				reply.stage = reduceTask.stage
				reply.files = reduceTask.files
				reply.nReduce = reduceTask.nReduce
				return nil
			}
		}

	}
	return nil
}

// ack from worker to confirm master's state
func (m *Master) confirmState(args *RequestAckArgs, reply *ReplyAckArgs) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if args.taskType == "MAP" {
		// update Master.reduceList.files
		var mapTaskid int
		for _, filename := range args.files {
			ret := strings.Split(filename, "-")
			mapTaskid, _ = strconv.Atoi(ret[1])
			y, _ := strconv.Atoi(ret[2])
			m.reduceList[y].files = append(m.reduceList[y].files, filename)
		}
		// update Master.mapList and isMapFinished
		m.mapList[mapTaskid].status = "FINISHED"
		for _, t := range m.mapList {
			if t.status != "FINISHED" {
				return nil
			}
		}
		m.isMapFinished = true
	} else if args.taskType == "REDUCE" {
		// update Master.isReduceFinished
		for _, filename := range args.files {
			ret := strings.Split(filename, "-")
			reduceTaskid, _ := strconv.Atoi(ret[2])
			m.reduceList[reduceTaskid].status = "FINISHED"
		}
		for _, t := range m.reduceList {
			if t.status != "FINISHED" {
				return nil
			}
		}
		m.isReduceFinished = true
	}

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
