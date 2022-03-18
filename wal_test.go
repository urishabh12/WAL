package wal

import (
	"fmt"
	"testing"
)

func Test_NewAndLoad(t *testing.T) {
	log := "test"
	err := New(log, 1000)
	handleErr(err, t)
	l, err := Load(log)
	handleErr(err, t)
	assertEqualInt(l.meta.LastSegNumber, 1, t)
	assertEqualInt(l.meta.MaxSegLength, 1000, t)
}

func Test_CreateAndLoadMetadata(t *testing.T) {
	log := "./test"
	err := createDirectory(log)
	handleErr(err, t)
	err = createMetadata(log, 10)
	handleErr(err, t)
	_, err = loadMetadata(log)
	handleErr(err, t)
}

func assertEqualInt(a int, b int, t *testing.T) {
	if a != b {
		t.Fatalf(fmt.Sprintf("%d is not equal to %d", a, b))
	}
}
