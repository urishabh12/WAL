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

	err = createFile(dirPath + "/1")

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

	seg, err := loadLastSegment(dirPath, meta)
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

func loadLastSegment(dirPath string, meta *Metadata) (*segment, error) {
	fileName := fmt.Sprintf("%d", meta.LastSegNumber)
	filePath := dirPath + "/" + fileName
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
		currentSeqNumber:   meta.LastSegNumber,
		filePath:           filePath,
		data:               segData,
		size:               len(segData),
	}, nil
}
