package wal

import (
	"fmt"
	"testing"

	"github.com/urishabh12/WAL/file_reader"
)

func Test_SegmentAppend(t *testing.T) {
	file, err := file_reader.OpenFile("testSA")
	handleErr(err, t)
	seg := segment{
		maxNumberOfRecords: 10,
		size:               0,
		data:               [][]byte{},
		currentSeqNumber:   1,
		filePath:           "1",
		file:               file,
		syncAfter:          0,
		lastSync:           0,
	}

	data := "Hello"
	for i := 0; i < 5; i++ {
		err := seg.append([]byte(data))
		handleErr(err, t)
	}
	resp, err := seg.get(2, 0)
	handleErr(err, t)
	for i := 0; i < len(resp); i++ {
		output := string(resp[i])
		assertEqualString(data, output, t)
	}
}

func Test_SegmentGet(t *testing.T) {
	file, err := file_reader.OpenFile("testSA")
	handleErr(err, t)
	seg := segment{
		maxNumberOfRecords: 10,
		size:               0,
		data:               [][]byte{},
		currentSeqNumber:   2,
		filePath:           "2",
		file:               file,
		syncAfter:          0,
		lastSync:           0,
	}

	data := "Hello"
	for i := 0; i < 5; i++ {
		err := seg.append([]byte(data))
		handleErr(err, t)
	}
	_, err = seg.get(2, 9)
	handleNotErr(err, t)
}

func handleErr(err error, t *testing.T) {
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func handleNotErr(err error, t *testing.T) {
	if err == nil {
		t.Fatalf("did not return error")
	}
}

func assertEqualString(a string, b string, t *testing.T) {
	if a != b {
		t.Fatalf(fmt.Sprintf("%s is not equal to %s", a, b))
	}
}
