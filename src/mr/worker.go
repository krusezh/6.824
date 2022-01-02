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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	// Request Task
	for {
		args := &TaskArgs{}
		reply := &TaskReply{}
		err := call("Master.AssignTask", args, reply)
		if err == false {
			break
		}

		// Handle Task
		// Map Task
		if reply.MTask != nil {
			task := reply.MTask
			//log.Printf("task = %+v\n", task)
			doMap(task, mapf)
			args = &TaskArgs{
				MTask: task,
			}
		} else if reply.RTask != nil {
			// Reduce Task
			task := reply.RTask
			//log.Printf("task = %+v\n", task)
			doReduce(task, reducef)
			args = &TaskArgs{
				RTask: task,
			}
		} else {
			// Do nothing
		}

		reply = &TaskReply{}
		call("Master.FinishTask", args, reply)
	}

}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) error {
	// read file
	content, _ := ioutil.ReadFile(task.FileName)
	// process
	kvs := mapf(task.FileName, string(content))
	// write json
	results := make([][]KeyValue, task.NReduce)
	for _, v := range kvs {
		idx := ihash(v.Key) % task.NReduce
		results[idx] = append(results[idx], v)
	}

	// for each reduce index
	// save intermediate data
	for idx, kvs := range results {
		outName := "mr-" + strconv.Itoa(task.Id()) + "-" + strconv.Itoa(idx)
		f, _ := os.Create(outName)
		encoder := json.NewEncoder(f)
		for _, kv := range kvs {
			encoder.Encode(&kv)
		}
		f.Close()
	}

	return nil
}

func doReduce(task *ReduceTask, reducef func(string, []string) string) error {
	reduceId := task.Id()
	mapIds := task.MapIds
	kvs := map[string][]string{}
	for i := 0; i < mapIds; i++ {
		inName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId)
		f, _ := os.Open(inName)
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		f.Close()
	}
	res := []KeyValue{}
	for k, v := range kvs {
		count := reducef(k, v)
		res = append(res, KeyValue{k, count})
	}

	sort.Slice(res, func(i, j int) bool { return res[i].Key < res[j].Key })

	outName := "mr-out-" + strconv.Itoa(reduceId)
	of, _ := os.Create(outName)
	for _, kv := range res {
		fmt.Fprintf(of, "%v %v\n", kv.Key, kv.Value)
	}
	return nil
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
