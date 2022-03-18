package wal

import (
	"github.com/urishabh12/WAL/file_reader"
)

const (
	delim = "\n"
)

type segment struct {
	maxNumberOfFiles int
	currentSeqNumber int
	filePath         string
	data             [][]byte
	size             int
}

type OutOfBoundError struct{}

func (o OutOfBoundError) Error() string {
	return "offset out of bound"
}

type SegmentFullError struct{}

func (s SegmentFullError) Error() string {
	return "segment capacity full"
}

//Append will first add to log segment file than to it's own in memory copy.
//This is done for fast lookup when trying to access immediate logs
func (s *segment) append(data []byte) error {
	if s.size == s.maxNumberOfFiles {
		return SegmentFullError{}
	}

	fileData := append(data, []byte(delim)...)

	err := file_reader.Append(s.filePath, fileData)
	if err != nil {
		return err
	}
	s.data = append(s.data, data)
	s.size++

	return nil
}

func (s *segment) get(total int, offset int) ([][]byte, error) {
	if offset >= s.size || offset < 0 || total < 1 {
		return nil, OutOfBoundError{}
	}

	resp := [][]byte{}
	end := min(offset+total, s.size-1)
	for i := offset; i <= end; i++ {
		resp = append(resp, s.data[i])
	}

	return resp, nil
}

func min(a int, b int) int {
	if a < b {
		return a
	}

	return b
}