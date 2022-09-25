package wal

import (
	"testing"

	file "github.com/urishabh12/WAL/file_manager"
)

func Test_Iterator(t *testing.T) {
	path := "testIter"
	defer file.Delete(path)
	createTestWAL(path, 10, 0, t)
	l, err := Load(path)
	handleErr(err, t)
	dataFirst := "First"
	dataSecond := "Second"
	for i := 0; i < 10; i++ {
		l.Add([]byte(dataFirst))
	}
	for i := 0; i < 10; i++ {
		l.Add([]byte(dataSecond))
	}

	iter, err := NewIterator(l)
	handleErr(err, t)
	for i := 0; i < 10; i++ {
		val := string(iter.Value)
		assertEqualString(dataSecond, val, t)
		err = iter.Next()
		handleErr(err, t)
	}
	val := string(iter.Value)
	assertEqualString(dataFirst, val, t)
	for i := 0; i < 9; i++ {
		err = iter.Next()
		handleErr(err, t)
		val := string(iter.Value)
		assertEqualString(dataFirst, val, t)
	}
}

func Test_IteratorNextOverflow(t *testing.T) {
	path := "testINOF"
	defer file.Delete(path)
	createTestWAL(path, 10, 0, t)
	l, err := Load(path)
	handleErr(err, t)
	data := "data"

	for i := 0; i < 5; i++ {
		l.Add([]byte(data))
	}

	iter, err := NewIterator(l)
	handleErr(err, t)
	for i := 0; i < 4; i++ {
		err := iter.Next()
		handleErr(err, t)
	}

	err = iter.Next()
	if !IsEndOfLogError(err) {
		t.Fatalf("did not return end of log error")
	}
}

func createTestWAL(name string, size int, syncAfter int, t *testing.T) {
	opt := Options{
		SegmentSize: size,
		SyncAfter:   syncAfter,
	}
	err := New(name, opt)
	handleErr(err, t)
}
