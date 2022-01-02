package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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
type TaskArgs struct {
	MTask *MapTask
	RTask *ReduceTask
}

type TaskReply struct {
	MTask *MapTask
	RTask *ReduceTask
}

type State byte

const (
	READY State = iota
	RUNNING
	FINISHED
)

type Task interface {
	Id() int
	State() State
	Time() time.Time

	SetId(id int)
	SetState(state State)
	SetTime()
}

type BaseTask struct {
	TaskId    int
	TaskState State
	StartTime time.Time
}

func (bt *BaseTask) Id() int {
	return bt.TaskId
}

func (bt *BaseTask) SetId(id int) {
	bt.TaskId = id
}
func (bt *BaseTask) Time() time.Time {
	return bt.StartTime
}

func (bt *BaseTask) State() State {
	return bt.TaskState
}

func (bt *BaseTask) SetTime() {
	bt.StartTime = time.Now()
}

func (bt *BaseTask) SetState(state State) {
	bt.TaskState = state
}

type MapTask struct {
	*BaseTask
	NReduce  int
	FileName string
}

type ReduceTask struct {
	*BaseTask
	MapIds int
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
