package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/urishabh12/WAL/file_reader"
)

const (
	metadataPath = "/metadata"
)

type Log struct {
	name        string
	path        string
	lastSegment *segment
	meta        *Metadata
}

type Metadata struct {
	MaxSegLength  int
	LastSegNumber int
}

func New(logName string, size int) error {
	dirPath := fmt.Sprintf("./%s", logName)
	err := createDirectory(dirPath)
	if err != nil {
		return err
	}

	err = createMetadata(dirPath, size)
	if err != nil {
		return err
	}

	err = createFile(fmt.Sprintf("%s/1", dirPath))

	return err
}

func Load(logName string) (*Log, error) {
	dirPath := fmt.Sprintf("./%s", logName)
	err := checkIfDirectoryAccesible(dirPath)
	if err != nil {
		return nil, err
	}

	meta, err := loadMetadata(dirPath)
	if err != nil {
		return nil, err
	}

	seg, err := getSegment(dirPath, meta, meta.LastSegNumber)
	if err != nil {
		return nil, err
	}

	return &Log{
		name:        logName,
		path:        dirPath,
		lastSegment: seg,
		meta:        meta,
	}, nil
}

func checkIfDirectoryAccesible(path string) error {
	_, err := os.Stat(path)
	return err
}

func createDirectory(dirPath string) error {
	err := os.Mkdir(dirPath, 0766)

	return err
}

func createMetadata(dirPath string, size int) error {
	meta := Metadata{
		MaxSegLength:  size,
		LastSegNumber: 1,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	file_reader.Write(dirPath+metadataPath, data)

	return nil
}

func createFile(filePath string) error {
	err := file_reader.Write(filePath, []byte(""))
	return err
}

func loadMetadata(dirPath string) (*Metadata, error) {
	data, err := file_reader.Read(dirPath + metadataPath)
	if err != nil {
		return nil, err
	}

	var meta Metadata
	err = json.Unmarshal(data, &meta)

	return &meta, err
}

func getSegment(path string, meta *Metadata, segNumber int) (*segment, error) {
	fileName := fmt.Sprintf("%d", segNumber)
	filePath := fmt.Sprintf("%s/%s", path, fileName)
	data, err := file_reader.Read(filePath)
	if err != nil {
		return nil, err
	}

	segData := bytes.Split(data, []byte(delim))
	if len(segData) > 0 {
		segData = segData[:len(segData)-1]
	}

	return &segment{
		maxNumberOfRecords: meta.MaxSegLength,
		currentSeqNumber:   segNumber,
		filePath:           filePath,
		data:               segData,
		size:               len(segData),
	}, nil
}

//Log receiver functions

func (l *Log) Add(data []byte) error {
	if l.lastSegment.size == l.meta.MaxSegLength {
		err := l.createNextSegment()
		if err != nil {
			return err
		}
	}
	err := l.lastSegment.append(data)
	return err
}

func (l *Log) GetLast(count int, offset int) ([][]byte, error) {
	var resp [][]byte
	totalSize := count
	currSegment := l.lastSegment

	for len(resp) < totalSize {
		segOut, err := currSegment.get(count, offset)
		if err != nil {
			return resp, err
		}

		resp = append(resp, segOut...)
		ln := len(resp)
		if ln < totalSize {
			count -= len(resp)
			offset = 0
		}

		if currSegment.currentSeqNumber == 1 {
			break
		}
		currSegment, err = getSegment(l.path, l.meta, currSegment.currentSeqNumber-1)
		if err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (l *Log) createNextSegment() error {
	nextSegNumber := l.meta.LastSegNumber + 1
	err := createFile(fmt.Sprintf("%s/%d", l.path, nextSegNumber))
	if err != nil {
		return err
	}

	l.meta.LastSegNumber++
	err = l.lastSegment.close()
	if err != nil {
		return err
	}

	l.lastSegment, err = getSegment(l.path, l.meta, nextSegNumber)

	return err
}
