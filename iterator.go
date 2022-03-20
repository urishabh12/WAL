package wal

import (
	"bytes"
	"fmt"

	"github.com/urishabh12/WAL/file_reader"
)

type LogIterator struct {
	Value     []byte
	currIndex int
	path      string
	seg       *segment
}

//Iterator points to the latest log at the time of iterator creation and goes back to the oldest log.
func NewIterator(l *Log) (*LogIterator, error) {
	var iter LogIterator
	iter.path = l.path
	iter.currIndex = l.lastSegment.size - 1
	iter.seg = copySegment(l.lastSegment)
	iter.Value = iter.seg.data[iter.currIndex]

	return &iter, nil
}

//Decrements index by one and sets value to current
func (i *LogIterator) Next() {
	i.currIndex--
	if i.currIndex < 0 {
		i.prevSegment()
	}
	i.Value = i.seg.data[i.currIndex]
}

//Sets to previous segment
//Stays same if last segment
func (i *LogIterator) prevSegment() {
	//if any error stay at the index
	i.currIndex = 0
	//if last segment return
	if i.seg.currentSeqNumber == 1 {
		return
	}

	//load segment without file
	currSeqNum := i.seg.currentSeqNumber - 1
	fileName := fmt.Sprintf("%d", currSeqNum)
	filePath := fmt.Sprintf("%s/%s", i.path, fileName)
	data, err := file_reader.Read(filePath)
	if err != nil {
		return
	}

	segData := bytes.Split(data, []byte(delim))
	if len(segData) > 0 {
		segData = segData[:len(segData)-1]
	}

	i.seg = &segment{
		maxNumberOfRecords: i.seg.maxNumberOfRecords,
		currentSeqNumber:   currSeqNum,
		filePath:           filePath,
		data:               segData,
		size:               len(segData),
		syncAfter:          i.seg.syncAfter,
		lastSync:           len(segData),
	}
}

//Creates a copy of segment
func copySegment(a *segment) *segment {
	if a == nil {
		return a
	}

	return &segment{
		maxNumberOfRecords: a.maxNumberOfRecords,
		currentSeqNumber:   a.currentSeqNumber,
		filePath:           a.filePath,
		data:               a.data,
		size:               a.size,
		syncAfter:          a.syncAfter,
		lastSync:           a.lastSync,
	}
}
