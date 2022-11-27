package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	default_size = 26214400 // 25 megabytes
)

type LogOptions struct {
	size uint64
}

type Log struct {
	options    *LogOptions // log options
	path       string      // abs path to logs directory
	mu         sync.Mutex  // lock for concurrency
	segments   []*segment  // all segments
	file       *os.File    // latest segment file to append logs
	startIndex uint64      // start index
	lastIndex  uint64      // last index
}

func New(name string, options *LogOptions) (*Log, error) {
	l_opts := &LogOptions{}

	if options == nil {
		l_opts.size = default_size
	}

	l := &Log{
		options: l_opts,
	}

	var err error
	l.path, err = filepath.Abs(fmt.Sprintf("./%s", name))
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(l.path, 0750); err != nil {
		return nil, err
	}

	if err := l.load(); err != nil {
		return nil, err
	}

	return l, nil
}

// loads all segments and initializes start and end index
func (l *Log) load() error {
	files, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}

	for _, f := range files {
		name := f.Name()
		if f.IsDir() {
			continue
		}

		if len(name) < 20 || len(name) > 20 {
			continue
		}

		index, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			return err
		}

		s := &segment{
			index: index,
			path:  filepath.Join(l.path, name),
		}
		l.segments = append(l.segments, s)
	}

	if len(l.segments) == 0 {
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.startIndex = 1
		l.lastIndex = 0
		l.file, err = openFile(l.path)
		if err != nil {
			return err
		}
		return nil
	}

	// load last segment file
	l.file, err = openFile(l.segments[len(l.segments)-1].path)
	if err != nil {
		return err
	}

	loadSegment(l.segments[len(l.segments)-1])
	l.startIndex = l.segments[0].index

	return nil
}

// loads data onto the segment
func loadSegment(s *segment) error {
	var err error
	s.bdata, err = os.ReadFile(s.path)
	if err != nil {
		return err
	}

	//load segment log index position
	data := s.bdata
	for len(data) > 0 {
		n, l_p, err := readNextEntryPos(data)
		if err != nil {
			return err
		}
		data = data[n:]
		s.lpos = append(s.lpos, l_p)
	}

	return nil
}

// reads first length int and check for corrupt data
// returns start and end pos of data and len of total entry
func readNextEntryPos(b []byte) (int, bpos, error) {
	return 0, bpos{}, nil
}

//opens file, if not exists creates it
func openFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0640)
}

// file name of len uint64 == 20
func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}
