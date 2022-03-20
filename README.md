# WAL
Fast and configurable write ahead log

## Example

### Basic WAL
```
name := log
opt := Options{
		SegmentSize: 1000000,
		SyncAfter:   1000,
	}
err := wal.New(name, opt)
l, err := wal.Load(name)
//handle err

l.Add([]byte("One"))
l.Add([]byte("Two"))
l.Add([]byte("Three"))

//3 is number of records & 0 is offset
resp, err := l.GetLast(3, 0)
```

### Log Iterator
```
iter, err := wal.NewIterator(l)
//handle err
for !wal.IsEndOfLogError(err) {
	val := string(iter.Value)
	fmt.Println(val)
	err = iter.Next()
}
```