package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := WorkerArgs{}
		reply := MasterReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok || reply.TaskType == 3 {
			break
		}
		if reply.TaskType == 0 { // map
			intermediate := []KeyValue{}
			// 读取文件，生成中间键值对
			file, err := os.Open(reply.MapFileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapFileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapFileName)
			}
			file.Close()
			kva := mapf(reply.MapFileName, string(content))
			intermediate = append(intermediate, kva...)
			// 将中间键值对按照hash值分桶
			buckets := make([][]KeyValue, reply.NReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				buckets[ihash(kv.Key)%reply.NReduce] = append(buckets[ihash(kv.Key)%reply.NReduce], kv)
			}
			// 将桶中的键值对写入文件，每个桶一个文件
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
				ofile, _ := ioutil.TempFile("", oname+"*")
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets[i] {
					err := enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			// 通知master完成map任务
			finishArgs := WorkerArgs{reply.MapTaskNumber, -1}
			finishReply := ExampleReply{}
			call("Coordinator.FinishMapTask", &finishArgs, &finishReply)
		} else if reply.TaskType == 1 { // reduce
			intermediate := []KeyValue{}
			// 读取文件，保存中间键值对
			for i := 0; i < reply.NMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// 将中间键值对按照key排序
			sort.Sort(ByKey(intermediate))
			// 将相同key的value放在一起，调用reduce函数，将结果写入文件
			oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
			ofile, _ := ioutil.TempFile("", oname+"*")
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()
			// 删除读取过的文件
			for i := 0; i < reply.NMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot open delete" + iname)
				}
			}
			// 通知master完成reduce任务
			finishArgs := WorkerArgs{-1, reply.ReduceTaskNumber}
			finishReply := ExampleReply{}
			call("Coordinator.FinishReduceTask", &finishArgs, &finishReply)
		}
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
