package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files []string

	NMap              int
	MapTasksFlag      []int
	FinishedMapAmount int

	NReduce              int
	ReduceTasksFlag      []int
	FinishedReduceAmount int

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *MasterReply) error {
	c.mutex.Lock()
	if c.FinishedMapAmount < c.NMap { // 分配map任务
		// 寻找未完成的map任务
		mapTaskNumber := -1
		for i := 0; i < c.NMap; i++ {
			if c.MapTasksFlag[i] == 0 {
				mapTaskNumber = i
				break
			}
		}
		// 所有map任务都被分配
		if mapTaskNumber == -1 {
			reply.TaskType = 2
			c.mutex.Unlock()
		} else {
			reply.TaskType = 0
			reply.MapTaskNumber = mapTaskNumber
			reply.MapFileName = c.Files[mapTaskNumber]
			reply.NReduce = c.NReduce
			c.MapTasksFlag[mapTaskNumber] = 1
			c.mutex.Unlock()
			go func() { // 防止worker挂掉，10s后将任务标记为未完成
				time.Sleep(time.Duration(10) * time.Second)
				c.mutex.Lock()
				if c.MapTasksFlag[mapTaskNumber] == 1 {
					c.MapTasksFlag[mapTaskNumber] = 0
				}
				c.mutex.Unlock()
			}()
		}
	} else if c.FinishedReduceAmount < c.NReduce { // 分配reduce任务
		// 寻找未完成的reduce任务
		reduceTaskNumber := -1
		for i := 0; i < c.NReduce; i++ {
			if c.ReduceTasksFlag[i] == 0 {
				reduceTaskNumber = i
				break
			}
		}
		// 所有reduce任务都被分配
		if reduceTaskNumber == -1 {
			reply.TaskType = 2
			c.mutex.Unlock()
		} else {
			reply.TaskType = 1
			reply.NMap = c.NMap
			reply.ReduceTaskNumber = reduceTaskNumber
			c.ReduceTasksFlag[reduceTaskNumber] = 1
			c.mutex.Unlock()
			go func() { // 防止worker挂掉，10s后将任务标记为未完成
				time.Sleep(time.Duration(10) * time.Second)
				c.mutex.Lock()
				if c.ReduceTasksFlag[reduceTaskNumber] == 1 {
					c.ReduceTasksFlag[reduceTaskNumber] = 0
				}
				c.mutex.Unlock()
			}()
		}
	} else {
		reply.TaskType = 3
		c.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) FinishMapTask(args *WorkerArgs, reply *ExampleReply) error {
	c.mutex.Lock()
	c.MapTasksFlag[args.FinishedMapTaskNumber] = 2
	c.FinishedMapAmount++
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) FinishReduceTask(args *WorkerArgs, reply *ExampleReply) error {
	c.mutex.Lock()
	c.ReduceTasksFlag[args.FinishedReduceTaskNumber] = 2
	c.FinishedReduceAmount++
	c.mutex.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.FinishedReduceAmount == c.NReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NMap = len(files)
	c.NReduce = nReduce
	c.MapTasksFlag = make([]int, c.NMap)
	c.ReduceTasksFlag = make([]int, c.NReduce)

	c.server()
	return &c
}
