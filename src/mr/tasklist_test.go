package mr

import (
	"reflect"
	"sync"
	"testing"
)

func Test_NewTaskManager(t *testing.T) {
	toTest := NewTaskManager([]string{"foo1", "foo2"}, 3)
	expected := taskmanager{
		l:           toTest.l,
		assignments: map[int]int{},
		tasks: []task{
			{
				ttype:      Map,
				tid:        taskId{0, 0},
				status:     IDLE,
				inputFiles: []string{"foo1"},
				assignedId: 0,
			},
			{
				ttype:      Map,
				tid:        taskId{1, 0},
				status:     IDLE,
				inputFiles: []string{"foo2"},
				assignedId: 0,
			},
			{
				ttype:      Reduce,
				tid:        taskId{2, 0},
				status:     IDLE,
				inputFiles: []string{"mr-0-0", "mr-1-0"},
				assignedId: 0,
			},
			{
				ttype:      Reduce,
				tid:        taskId{3, 1},
				status:     IDLE,
				inputFiles: []string{"mr-0-1", "mr-1-1"},
				assignedId: 0,
			},
			{
				ttype:      Reduce,
				tid:        taskId{4, 2},
				status:     IDLE,
				inputFiles: []string{"mr-0-2", "mr-1-2"},
				assignedId: 0,
			},
		},
		totalCompleted:    0,
		mapTasksCompleted: 0,
		numMapTasks:       2,
		numReducers:       3,
	}

	if !reflect.DeepEqual(*toTest, expected) {
		t.Errorf("taskmanager initialization expected: %v, received: %v", expected, toTest)
	}
}

func Test_taskmanager_unassignTask(t *testing.T) {
	toTest := taskmanager{
		l:           &sync.Mutex{},
		assignments: map[int]int{8: 0},
		tasks: []task{
			{
				ttype:      Map,
				tid:        taskId{0, 0},
				status:     INPROGRESS,
				inputFiles: []string{"foo1"},
				assignedId: 8,
			},
			{
				ttype:      Map,
				tid:        taskId{1, 0},
				status:     IDLE,
				inputFiles: []string{"foo2"},
				assignedId: 0,
			},
		},
	}

	expected := taskmanager{
		l:           toTest.l,
		assignments: map[int]int{},
		tasks: []task{
			{
				ttype:      Map,
				tid:        taskId{0, 0},
				status:     IDLE,
				inputFiles: []string{"foo1"},
				assignedId: 8,
			},
			{
				ttype:      Map,
				tid:        taskId{1, 0},
				status:     IDLE,
				inputFiles: []string{"foo2"},
				assignedId: 0,
			},
		},
	}

	err := toTest.unassignTask(8)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(toTest, expected) {
		t.Errorf("taskmanager unassign expected: %v, received: %v", expected, toTest)
	}
}

func Test_taskmanager_completeTask(t *testing.T) {
	toTest := taskmanager{
		l:                 &sync.Mutex{},
		assignments:       map[int]int{8: 0},
		mapTasksCompleted: 0,
		totalCompleted:    0,
		tasks: []task{
			{
				ttype:      Map,
				tid:        taskId{0, 0},
				status:     INPROGRESS,
				inputFiles: []string{"foo1"},
				assignedId: 8,
			},
			{
				ttype:      Map,
				tid:        taskId{1, 0},
				status:     IDLE,
				inputFiles: []string{"foo2"},
				assignedId: 0,
			},
		},
	}

	expected := taskmanager{
		l:                 toTest.l,
		assignments:       map[int]int{},
		mapTasksCompleted: 1,
		totalCompleted:    1,
		tasks: []task{
			{
				ttype:      Map,
				tid:        taskId{0, 0},
				status:     COMPLETED,
				inputFiles: []string{"foo1"},
				assignedId: 8,
			},
			{
				ttype:      Map,
				tid:        taskId{1, 0},
				status:     IDLE,
				inputFiles: []string{"foo2"},
				assignedId: 0,
			},
		},
	}

	err := toTest.completeTask(8)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(toTest, expected) {
		t.Errorf("taskmanager complete expected: %v, received: %v", expected, toTest)
	}
}

/*
type TestParams struct {
	input      int
	expected   task
	expectedOk bool
}

func Test_AssignTaskSequential(t *testing.T) {

	tests := []struct {
		first  taskmanager
		second []TestParams
	}{
		// all tasks available and are assigned to new workers
		// additional workers don't get worker
		{
			taskmanager{
				l: &sync.Mutex{},
				tasks: []task{
					{
						id:     0,
						status: IDLE,
						input:  "f1",
					},
					{
						id:     1,
						status: IDLE,
						input:  "f2",
					},
					{
						id:     2,
						status: IDLE,
						input:  "f3",
					}},
				assignments: map[int]int{},
				numReducers: 5,
			},
			[]TestParams{
				{
					1,
					task{0, taskId{0,0}, INPROGRESS, []string{"f1"},1},
					true,
				},
				{
					2,
					mapTask{1, 2, INPROGRESS, "f2"},
					true,
				},
				{
					3,
					mapTask{2, 3, INPROGRESS, "f3"},
					true,
				},

				// a fourth worker shouldn't get work because all mapTasks are in progress
				{
					4,
					mapTask{},
					false,
				},
			},
		},

		// return the assign task if it is already assigned to the current requestor
		{
			taskmanager{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						worker: 5,
						status: INPROGRESS,
						input:  "f1",
					},
				},
				assignments: map[int]int{5: 0},
			},
			[]TestParams{
				{
					5,
					mapTask{0, 5, INPROGRESS, "f1"},
					true,
				},
			},
		},

		// assign the last inprogress to a new worker, and the next new worker shouldn't get
		// any work
		{
			taskmanager{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						worker: 1,
						status: INPROGRESS,
						input:  "f1",
					},
					{
						id:     1,
						status: IDLE,
						input:  "f2",
					},
					{
						id:     2,
						worker: 3,
						status: INPROGRESS,
						input:  "f3",
					},
				},
				assignments: map[int]int{1: 0, 3: 2},
			},
			[]TestParams{
				{
					1,
					mapTask{0, 1, INPROGRESS, "f1"},
					true,
				},
				{
					2,
					mapTask{1, 2, INPROGRESS, "f2"},
					true,
				},
				{
					4,
					mapTask{},
					false,
				},
			},
		},
		// one idle task, remainder completed
		{
			taskmanager{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						status: COMPLETED,
						input:  "f1",
					},
					{
						id:     1,
						status: IDLE,
						input:  "f2",
					},
					{
						id:     2,
						status: COMPLETED,
						input:  "f3",
					},
				},
				assignments: map[int]int{},
			},
			[]TestParams{
				{
					1,
					mapTask{1, 1, INPROGRESS, "f2"},
					true,
				},
				{
					4,
					mapTask{},
					false,
				},
			},
		},

		// all inprogress tasks
		{
			taskmanager{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						worker: 8,
						status: INPROGRESS,
						input:  "f1",
					},
					{
						id:     1,
						worker: 11,
						status: INPROGRESS,
						input:  "f2",
					},
				},
				assignments: map[int]int{8: 0, 11: 1},
			},
			[]TestParams{
				{
					1,
					mapTask{},
					false,
				},
			},
		},

		// all completed tasks
		{
			taskmanager{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						status: COMPLETED,
						input:  "f1",
					},
					{
						id:     1,
						status: COMPLETED,
						input:  "f2",
					},
				},
				assignments: map[int]int{},
			},
			[]TestParams{
				{
					1,
					mapTask{},
					false,
				},
			},
		},
	}

	for _, tt := range tests {
		toTest := tt.first
		testParamsList := tt.second
		for _, ttt := range testParamsList {
			actual, ok := toTest.assignMapTask(ttt.input)
			if actual != ttt.expected {
				t.Errorf("assignMapTask(%d): expected %v, actual %v", ttt.input, ttt.expected, actual)

			}

			if ok != ttt.expectedOk {
				t.Errorf("assignMapTask(%d): expected %t, actual %t", ttt.input, ttt.expectedOk, ok)

			}
		}



			actual, ok = workload.getAssignedMapTask(tt.n)
			if actual != tt.expected {
				t.Errorf("getAssignedMapTask(%d): expected %v, actual %v", tt.n, tt.expected, actual)

			}

			if ok != tt.expectedOk {
				t.Errorf("getAsssignedMapTask(%d): expected %t, actual %t", tt.n, tt.expectedOk, ok)

			}

	}

}

func Test_AssignTaskConcurrent(t *testing.T) {

	toTest := taskmanager{
		l: &sync.Mutex{},
		mapTasks: []mapTask{
			{
				id:     0,
				status: IDLE,
				input:  "f1",
			},
			{
				id:     1,
				status: INPROGRESS,
				input:  "f2",
			},
			{
				id:     2,
				status: IDLE,
				input:  "f2",
			},
			{
				id:     3,
				status: INPROGRESS,
				input:  "f2",
			},
			{
				id:     4,
				status: IDLE,
				input:  "f2",
			},
			{
				id:     5,
				status: INPROGRESS,
				input:  "f2",
			},
		},
		assignments: map[int]int{1: 1, 3: 3, 5: 5},
	}
	doTask := func(wid int) {
		toTest.assignMapTask(wid)

	}

	unassignTask := func(wid int) {
		toTest.unassignMapTask(wid)

	}
	for i := 0; i < 10; i++ {
		if (i % 2) == 0 {
			go doTask(i)
		} else {
			go unassignTask(i)
		}
	}

	time.Sleep(5 * time.Second)
}
*/

/*
func Test_taskmanager_unassignTask(t *testing.T) {
	type fields struct {
		l                 *sync.Mutex
		assignments       map[int]int
		tasks             []task
		totalCompleted    int
		mapTasksCompleted int
		numMapTasks       int
		numReducers       int
	}
	type args struct {
		workerId int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &taskmanager{
				l:                 tt.fields.l,
				assignments:       tt.fields.assignments,
				tasks:             tt.fields.tasks,
				totalCompleted:    tt.fields.totalCompleted,
				mapTasksCompleted: tt.fields.mapTasksCompleted,
				numMapTasks:       tt.fields.numMapTasks,
				numReducers:       tt.fields.numReducers,
			}
			if err := tr.unassignTask(tt.args.workerId); (err != nil) != tt.wantErr {
				t.Errorf("taskmanager.unassignTask() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
*/
