package mr

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	// Your definitions here.
	// File list
	files   []string
	nReduce int

	taskList []Task

	// task queue
	taskQueue chan Task

	ctx    context.Context
	cancel context.CancelFunc
	done   bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	select {
	case task := <-m.taskQueue:
		task.SetState(RUNNING)
		task.SetTime()
		m.taskList[task.Id()] = task
		if t, ok := task.(*MapTask); ok {
			reply.MTask = t
		} else if t, ok := task.(*ReduceTask); ok {
			reply.RTask = t
		} else {
			// Do nothing
		}
		//log.Printf("task = %+v\n", task)

	case <-m.ctx.Done():
		return errors.New("Task is over, please quit.")
	}
	return nil
}

func (m *Master) FinishTask(args *TaskArgs, reply *TaskReply) error {
	var task Task
	if args.MTask != nil {
		task = args.MTask
	} else {
		task = args.RTask
	}
	m.taskList[task.Id()].SetState(FINISHED)
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

func (m *Master) monitor() {
	finished := 0
	for {
		for i := 0; i < len(m.taskList); i++ {
			if m.taskList[i] == nil {
				continue
			}
			if m.taskList[i].State() == READY {
				//do nothing
			} else if m.taskList[i].State() == RUNNING {
				if time.Now().Sub(m.taskList[i].Time()).Seconds() > 10 {
					m.taskList[i].SetState(READY)
					m.taskQueue <- m.taskList[i]
				}
			} else {
				//finished
				finished++
			}

		}
		if len(m.taskList) == finished {
			break
		} else {
			finished = 0
		}

		time.Sleep(time.Second)
	}
}

func (m *Master) dispatchTask() {
	go func() {
		for i := 0; i < len(m.files); i++ {
			task := &MapTask{
				NReduce:  m.nReduce,
				FileName: m.files[i],
				BaseTask: &BaseTask{
					TaskId:    i,
					TaskState: READY,
				},
			}
			m.taskQueue <- task
		}
	}()
	m.monitor()

	// Dispatch Reduce task
	m.taskList = make([]Task, m.nReduce)
	go func() {
		for i := 0; i < m.nReduce; i++ {
			task := &ReduceTask{
				MapIds: len(m.files),
				BaseTask: &BaseTask{
					TaskId:    i,
					TaskState: READY,
				},
			}
			m.taskQueue <- task
		}
	}()
	m.monitor()

	m.cancel()
	m.done = true
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	ctx, cancel := context.WithCancel(context.Background())
	m := Master{
		files:     files,
		nReduce:   nReduce,
		taskList:  make([]Task, len(files)),
		taskQueue: make(chan Task, 1),
		ctx:       ctx,
		cancel:    cancel,
		done:      false,
	}

	// Your code here.
	go m.dispatchTask()

	m.server()
	return &m
}
