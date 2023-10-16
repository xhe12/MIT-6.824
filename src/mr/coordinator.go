package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int8
type WorkerStatus int8
type FileName string
type TaskType int8

// type WorkerMap map[FileName]WorkerInfo
//type TaskMap

// const (
// 	idle WorkerStatus = iota
// 	busy
// )

const (
	mapTask TaskType = iota
	reduceTask
)
const (
	Assigned TaskStatus = iota
	Complete
	Unprocessed
)

type Task struct {
	TaskID       int
	FileName     FileName
	TaskType     TaskType
	Status       TaskStatus
	MapWorkerPID []int // pid of the mapper who is doing the job or pids of the mappers who completed the job
}

type TaskInfo struct {
	TaskID    int
	WorkerPID int
	Status    TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    chan Task
	ReduceTasks chan Task
	TaskDB      map[int]TaskInfo
	mu          sync.Mutex
	mappers     []int
	nReduce     int
	nMap        int
	mapWG       sync.WaitGroup
	reduceWG    sync.WaitGroup

	done bool
	//nMapWorkers   int
}

// RPC handlers for the worker to call.
func (c *Coordinator) SendTaskNumber(args int, reply *TaskNumberReply) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}
func (c *Coordinator) AssignMapTaskReq(args int, reply *TaskReqReply) error {
	task, ok := <-c.MapTasks
	// check if the MapTasks channel is open
	// if open, then assign a map task to worker
	if ok {
		task.Status = Assigned // task status
		task.MapWorkerPID = append(task.MapWorkerPID, args)
		*reply = TaskReqReply{task}

		go func() {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			_, exists := c.TaskDB[task.TaskID]

			if !exists {
				// Add this task to the MapTasks queue
				c.MapTasks <- Task{task.TaskID, task.FileName, mapTask, Unprocessed, []int{}}

			}
			c.mu.Unlock()

		}()
	} else {
		// if channel is closed, return an error
		return errors.New("all map tasks are finished")
	}
	return nil
}

func (c *Coordinator) AssignReduceTaskReq(args int, reply *TaskReqReply) error {
	task, ok := <-c.ReduceTasks
	// check if the MapTasks channel is open
	// if open, then assign a map task to worker
	if ok {
		task.Status = Assigned // task status
		*reply = TaskReqReply{task}
		//fmt.Printf("Assigning task %v \n", task)
		go func() {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			_, exists := c.TaskDB[task.TaskID]

			if !exists {
				// Add this task to the ReduceTasks queue
				c.ReduceTasks <- Task{task.TaskID, "", reduceTask, Unprocessed, c.mappers}
				//fmt.Printf("Adding reduce task: %v\n", task)
			}
			c.mu.Unlock()

		}()
	} else {
		// if channel is closed, return an error
		return errors.New("all reduce tasks are finished")
	}
	return nil
}

func (c *Coordinator) MapTaskNotice(args *Task, reply *struct{}) error {
	if args.Status == Complete {

		c.mu.Lock()
		_, exists := c.TaskDB[args.TaskID]
		// if TaskDB[args.FileName] exists, then the current worker is a strangler
		// and the map task is completed by another worker.
		// Do not change the TaskDB in this case
		// if TaskDB[args.FileName] doesn't exist, then add the current worker info to TaskDB
		if !exists {
			c.TaskDB[args.TaskID] = TaskInfo{args.TaskID, args.MapWorkerPID[0], Complete}
			c.mapWG.Done()
			//fmt.Printf("task %v is complete\n", args)
		}

		c.mu.Unlock()
	}

	return nil
}

func (c *Coordinator) ReduceTaskNotice(args *Task, reply *struct{}) error {
	if args.Status == Complete {
		c.mu.Lock()
		_, exists := c.TaskDB[args.TaskID]
		// if TaskDB[args.FileName] exists, then the current worker is a strangler
		// and the map task is completed by another worker.
		// Do not change the TaskDB in this case
		// if TaskDB[args.FileName] doesn't exist, then add the current worker info to TaskDB
		if !exists && args.Status == Complete {
			c.TaskDB[args.TaskID] = TaskInfo{args.TaskID, -1, Complete}
			c.reduceWG.Done()
			//fmt.Printf("Reduce Task %v is done\n", args.TaskID)
		}

		c.mu.Unlock()
	}

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
	//fmt.Println("Listening on socket: " + sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{done: false}
	c.nMap = 0
	for range files {
		c.nMap++
	}
	// Create Map Tasks
	c.MapTasks = make(chan Task, c.nMap)

	c.nReduce = nReduce
	c.ReduceTasks = make(chan Task, nReduce)
	c.TaskDB = make(map[int]TaskInfo)

	c.mapWG.Add(c.nMap)
	for i, file := range files {
		c.MapTasks <- Task{i, FileName(file), mapTask, Unprocessed, []int{}}
	}
	//fmt.Println("Starting the server...")
	c.server()
	go func() {
		c.mapWG.Wait()
		close(c.MapTasks)
		// iterate over TaskDB and found out the mappers' PIDs
		for id := 0; id < c.nMap; id++ {
			c.mappers = append(c.mappers, c.TaskDB[id].WorkerPID)
		}
		// Create Reduce Tasks after all the map tasks are finished
		for i := 0; i < c.nReduce; i++ {
			c.reduceWG.Add(1)
			task := Task{i + c.nMap, "", reduceTask, Unprocessed, c.mappers}
			c.ReduceTasks <- task
		}
		go func() {
			c.reduceWG.Wait()
			close(c.ReduceTasks)
			c.mu.Lock()
			c.done = true
			c.mu.Unlock()
		}()
	}()

	return &c
}
