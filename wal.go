package wal

type Log struct {
	name        string
	lastSegment *segment
	meta        *metadata
}

type metadata struct {
	maxSegLength int
}

func Load(logName string) {

}
