package wal

type LogIterator struct {
	Value     []byte
	currIndex int
	path      string
	seg       *segment
	meta      *Metadata
}

var endOfLogErrorText string = "Log file has ended"

type EndOfLog struct{}

func (e EndOfLog) Error() string {
	return endOfLogErrorText
}

func IsEndOfLogError(e error) bool {
	if e == nil {
		return false
	}
	return e.Error() == endOfLogErrorText
}

//Iterator points to the latest log at the time of iterator creation and goes back to the oldest log.
func NewIterator(l *Log) (*LogIterator, error) {
	var iter LogIterator
	iter.path = l.path
	iter.currIndex = l.lastSegment.size - 1
	iter.seg = copySegment(l.lastSegment)
	iter.Value = iter.seg.data[iter.currIndex]
	iter.meta = l.meta

	return &iter, nil
}

//Decrements index by one and sets value to current
func (i *LogIterator) Next() error {
	i.currIndex--
	if i.currIndex < 0 {
		err := i.prevSegment()
		if err != nil {
			return err
		}
	}

	i.Value = i.seg.data[i.currIndex]
	return nil
}

//Sets to previous segment
func (i *LogIterator) prevSegment() error {
	//if last segment return
	if i.seg.currentSeqNumber == 1 {
		return EndOfLog{}
	}

	//load segment without file
	currSeqNum := i.seg.currentSeqNumber - 1
	prevSeg, err := getSegment(i.path, i.meta, currSeqNum)
	if err != nil {
		return err
	}

	i.seg = prevSeg
	i.path = prevSeg.filePath
	i.currIndex = prevSeg.size - 1

	return nil
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
