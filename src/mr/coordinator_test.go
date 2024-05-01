package mr

import (
	"testing"
)

func Test_RequestTaskSequential(t *testing.T) {
	inputs := []string{"f1", "f2", "f3", "f4"}

	toTest := NewCoordinator(inputs, 10)

	for i, item := range inputs {
		request := RequestTaskArgs{ClientId: i}
		response := RequestTaskReply{}
		toTest.RequestTask(&request, &response)
		if response.Type != Map {
			t.Error("expected assigned task to be: map, got ", response.Type)

		}
		if response.MapParams.InputFile != item {
			t.Errorf("expected assigned input file to be %s, got %s", item, response.MapParams.InputFile)
		}
	}

}

/*
func Test_AssignNewTasksSequentialNoNewTasksAvailable(t *testing.T) {
	tasks := []task{{Status: INPROGRESS}, {Status: INPROGRESS}, {Status: COMPLETED}}
	toTest := Coordinator{
		NumReducers: 10,
		isDone:      false,
		tasklist: tasklist{
			tasks:       tasks,
			assignments: map[int]*task{}},
	}
	t, ok := toTest.assignNewTask(1)

}
*/
