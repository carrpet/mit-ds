package mr

import (
	"sync"
	"testing"
	"time"
)

type TestParams struct {
	input      int
	expected   mapTask
	expectedOk bool
}

func Test_AssignMapTaskSequential(t *testing.T) {

	tests := []struct {
		first  tasks
		second []TestParams
	}{
		// all tasks available and are assigned to new workers
		// additional workers don't get worker
		{
			tasks{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
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
				mapAssignments: map[int]int{},
				numReducers:    5,
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
			tasks{
				l: &sync.Mutex{},
				mapTasks: []mapTask{
					{
						id:     0,
						worker: 5,
						status: INPROGRESS,
						input:  "f1",
					},
				},
				mapAssignments: map[int]int{5: 0},
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
			tasks{
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
				mapAssignments: map[int]int{1: 0, 3: 2},
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
			tasks{
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
				mapAssignments: map[int]int{},
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
			tasks{
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
				mapAssignments: map[int]int{8: 0, 11: 1},
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
			tasks{
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
				mapAssignments: map[int]int{},
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

	/*
		tasksFactory := &tasksFactory{
			input: []string{"f1", "f2", "f3"},
		}
	*/
	//workload := tasksFactory.createWorkload()
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

		/*

			actual, ok = workload.getAssignedMapTask(tt.n)
			if actual != tt.expected {
				t.Errorf("getAssignedMapTask(%d): expected %v, actual %v", tt.n, tt.expected, actual)

			}

			if ok != tt.expectedOk {
				t.Errorf("getAsssignedMapTask(%d): expected %t, actual %t", tt.n, tt.expectedOk, ok)

			}
		*/
	}

}

func Test_AssignTaskConcurrent(t *testing.T) {

	toTest := tasks{
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
		mapAssignments: map[int]int{1: 1, 3: 3, 5: 5},
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
