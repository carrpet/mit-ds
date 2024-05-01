package mr

import (
	"bufio"
	"io"
	"reflect"
	"strings"
	"testing"
)

type testfile struct {
	fileStr string
	reader  *strings.Reader
}

func NewTestfile(contents []string) testfile {
	fileStr := strings.Join(contents, "\n")
	reader := strings.NewReader(fileStr)
	return testfile{fileStr, reader}
}
func (t testfile) Read(p []byte) (n int, err error) {
	n, err = t.reader.Read(p)
	return

}

func TestWorker(t *testing.T) {
	toTest := NewTestfile([]string{"A 1", "As 1", "B 1"})
	expected := "A 1\nAs 1\nB 1"

	strbuf := make([]byte, 100)
	numRead, err := toTest.Read(strbuf)
	if err != nil {
		t.Error("Could not read file!")
	}
	output := string(strbuf[:numRead])
	if output != expected {
		t.Errorf("expected: %s, got %s with numRead %d", expected, output, numRead)
	}

}

func TestNewTestFileScan(t *testing.T) {
	files := []io.Reader{
		NewTestfile([]string{"A 1", "As 1", "As 1"}),
	}
	toTest := bufio.NewScanner(files[0])

	toTest.Scan()
	toTest.Text()
	if toTest.Text() != "A 1" {
		t.Error("error scanning")
	}
	toTest.Scan()
	toTest.Text()
	if toTest.Text() != "As 1" {
		t.Error("error scanning as")
	}
	toTest.Scan()
	toTest.Text()
	if toTest.Text() != "As 1" {
		t.Errorf("error scanning, expected %s got %s", "As 1", toTest.Text())
	}
	if toTest.Scan() {
		t.Error("should return false!")
	}

}

func TestWorkerScan(t *testing.T) {
	files := []io.Reader{
		NewTestfile([]string{"A 1", "As 1", "As 1"}),
	}

	expected := []struct {
		key   string
		vals  []string
		isEOF bool
	}{{"A", []string{"1"}, false}, {"As", []string{"1", "1"}, true}}

	toTest := bufio.NewScanner(files[0])

	if !toTest.Scan() {
		t.Error("scan returned false, expected true")
	}

	for _, expect := range expected {
		reskey, resvals, resEOF := processNextKey(toTest)
		if reskey != expect.key {
			t.Errorf("wrong key, expected %s got %s!", expect.key, reskey)

		}
		if !reflect.DeepEqual(expect.vals, resvals) {
			t.Error("wrong vals!")

		}
		if resEOF != expect.isEOF {
			t.Error("wrong EOF state!")

		}
	}

}

func TestReduce(t *testing.T) {
	files := []io.Reader{
		NewTestfile([]string{"A 1", "As 1", "As 1"}),
		NewTestfile([]string{"As 1", "B 1", "C 1"}),
		NewTestfile([]string{"Ab 1", "Ae 1", "As 1"}),
	}

	expected := []keyvalues{
		{
			"A",
			[]string{"1"},
		},
		{
			"Ab",
			[]string{"1"},
		},
		{
			"Ae",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1", "1"},
		},
		{
			"B",
			[]string{"1"},
		},
		{
			"C",
			[]string{"1"},
		},
	}

	actual, ok := reduceWork(files)
	if !ok {
		t.Error("ReduceWork returned error!")
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected: %v, Actual: %v", expected, actual)
	}
}

func TestAccumulateResult(t *testing.T) {
	input := []keyvalues{
		{
			"A",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1", "1"},
		},
		{
			"B",
			[]string{"1"},
		},
	}

	expected := []keyvalues{
		{
			"A",
			[]string{"1"},
		},
		{
			"As",
			[]string{"1", "1", "1", "1"},
		},
		{
			"B",
			[]string{"1"},
		},
	}

	actual := accumulateResults(input)

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected: %v, Actual: %v", expected, actual)
	}
}
