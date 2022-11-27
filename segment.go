package wal

import (
	"errors"
)

var (
	// Index out of bound
	ErrOutOfBound = errors.New("index out of bound")
)

// segment will be used to store part of log
type segment struct {
	path  string // abs path to segment file
	index uint64 // starting index of segment
	bdata []byte // loaded binary data
	lpos  []bpos // log position in binary data
}

// binary position in binary array
type bpos struct {
	start uint64 // start position of binary data
	end   uint64 // end pos of binary data + 1
}

//get log at pos s.index-index
func (s *segment) getLog(index uint64) ([]byte, error) {
	seg_index := index - s.index
	if seg_index > uint64(len(s.lpos))-1 {
		return nil, ErrOutOfBound
	}
	return s.bdata[s.lpos[seg_index].start:s.lpos[seg_index].end], nil
}
