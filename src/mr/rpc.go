package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type WorkerHost struct {
	ip string
	port int
}

type Task struct {
	id int
	stage string // MAP, REDUCE, WAIT, EXIT
	files []string
	status string // NOTASSIGNED, ASSIGNED, FINISHED
	allocateTime int64
	nReduce int
}

// request of worker ask for task
type RequestTaskArgs struct {
}

// request after worker run tasks
type RequestAckArgs struct {
	taskType string // MAP, REDUCE
	files []string
}

// reply for RequestAckArgs
type ReplyAckArgs struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
