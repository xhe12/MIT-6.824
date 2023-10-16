package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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

	// Get Map Task
	//reply := TaskReqReply{}
	var reply TaskReqReply
	numberReply := TaskNumberReply{0, 0}
	pid := os.Getpid()

	call("Coordinator.SendTaskNumber", 1, &numberReply)
	//fmt.Printf("nMap = %v, nReduce = %v\n", numberReply.NMap, numberReply.NReduce)

	nMap := numberReply.NMap
	nReduce := numberReply.NReduce
	for {
		time.Sleep(time.Millisecond * 10)
		ok := call("Coordinator.AssignMapTaskReq", &pid, &reply)
		if ok {
			//run map task
			err := doMap(mapf, &reply.Task, nReduce, nMap)
			// notify coordinator
			if err != nil {
				reply.Task.Status = Unprocessed
			} else {
				reply.Task.Status = Complete
			}
			call("Coordinator.MapTaskNotice", &reply.Task, &struct{}{})

		} else {
			// ok is false when no map tasks left to be processed
			//fmt.Printf("no map tasks left to be processed\n")
			break // Break out of the for loop. Stop calling coordinator for tasks
		}
	}

	var reduceReply TaskReqReply
	for {
		time.Sleep(time.Millisecond * 10)
		ok := call("Coordinator.AssignReduceTaskReq", &pid, &reduceReply)
		if ok {
			// run reduce task
			//fmt.Printf("reducing - running task %v\n", reduceReply.Task)
			err := doReduce(reducef, &reduceReply.Task, nMap)
			// notify coordinator
			if err != nil {
				reduceReply.Task.Status = Unprocessed
			} else {
				reduceReply.Task.Status = Complete
			}
			call("Coordinator.ReduceTaskNotice", &reduceReply.Task, &struct{}{})
			//fmt.Printf("reducing - done with task %v\n", reduceReply.Task)
		} else {
			break
		}
	}

}

func doMap(mapf func(string, string) []KeyValue, task *Task, nReduce int, nMap int) error {
	file, err := os.Open(string(task.FileName))
	if err != nil {
		fmt.Printf("cannot open %v when mapping: ", file)
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v when mapping: ", file)
		return err
	}
	file.Close()
	kva := mapf(string(task.FileName), string(content))

	// Create /var/tmp/mr-mapTaskN-reduceTaskN-mapperPID files
	// and corresponding encoders
	var encoders []json.Encoder
	//var files []os.File
	for i := nMap; i < nReduce+nMap; i++ {
		filename := "/var/tmp/mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.MapWorkerPID[0]) + ".json"
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		//fmt.Printf("mapping - creating file %v\n", filename)
		if err != nil {
			fmt.Printf("cannot open %v with error %v", "/var/tmp/mr-"+strconv.Itoa(task.TaskID)+"-"+strconv.Itoa(i)+".json", err)
			return err
		}
		encoders = append(encoders, *json.NewEncoder(file))
		//files = append(files, *file)
		defer file.Close()
	}
	// write the mapping results kva to local disk
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		err := encoders[i].Encode(kv)
		if err != nil {
			fmt.Printf("Error when Encoding: %v", err)
		}
	}
	return nil
}

func doReduce(reducef func(string, []string) string, task *Task, nMap int) error {
	var kva []KeyValue
	for mapInd := 0; mapInd < nMap; mapInd++ {
		filename := "/var/tmp/mr-" + strconv.Itoa(mapInd) + "-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(task.MapWorkerPID[mapInd]) + ".json"
		file, err := os.Open(filename)

		if err != nil {
			fmt.Printf("Can't open file with error: %v", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(task.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Printf("Unable to create file %v with error %v\n", oname, ofile)
	}

	//
	// call Reduce on each distinct key in kva,
	// and print the result to mr-out-TaskID.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

//type Mapper struct{}

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
