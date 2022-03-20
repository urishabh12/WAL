package wal

import (
	"fmt"
	"testing"
)

func Test_NewAndLoad(t *testing.T) {
	log := "testNAL"
	opt := Options{
		SegmentSize: 1000,
	}
	err := New(log, opt)
	handleErr(err, t)
	l, err := Load(log)
	handleErr(err, t)
	assertEqualInt(l.meta.LastSegNumber, 1, t)
	assertEqualInt(l.meta.MaxSegLength, 1000, t)
}

func Test_CreateAndLoadMetadata(t *testing.T) {
	log := "./testCALM"
	err := createDirectory(log)
	handleErr(err, t)
	opt := Options{
		SegmentSize: 1000,
	}
	err = createMetadata(log, opt)
	handleErr(err, t)
	_, err = loadMetadata(log)
	handleErr(err, t)
}

func Test_AddAndGetLog(t *testing.T) {
	log := "testAAGL"
	opt := Options{
		SegmentSize: 5,
	}
	err := New(log, opt)
	handleErr(err, t)
	l, err := Load(log)
	handleErr(err, t)
	data := "Hello"
	for i := 0; i < 10; i++ {
		err = l.Add([]byte(data))
		handleErr(err, t)
	}

	resp, err := l.GetLast(3, 0)
	handleErr(err, t)
	if len(resp) != 3 {
		t.Fatalf(fmt.Sprintf("resp length is %d not 3", len(resp)))
	}

	for i := 0; i < len(resp); i++ {
		assertEqualString(string(resp[i]), data, t)
	}
}

func assertEqualInt(a int, b int, t *testing.T) {
	if a != b {
		t.Fatalf(fmt.Sprintf("%d is not equal to %d", a, b))
	}
}
