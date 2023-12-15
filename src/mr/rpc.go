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
type WorkerArgs struct {
	FinishedMapTaskNumber    int // 完成的map任务编号
	FinishedReduceTaskNumber int // 完成的reduce任务编号
}

type MasterReply struct {
	TaskType int // 0: map, 1: reduce, 2: wait, 3: exit

	NMap          int    // map任务的数量
	MapTaskNumber int    // map任务的编号
	MapFileName   string // map任务读取的文件名

	NReduce          int // reduce任务的数量
	ReduceTaskNumber int // reduce任务的编号
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
