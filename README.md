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
err := New(name, opt)
l, err := Load(name)
//handle err

l.Add([]byte("One"))
l.Add([]byte("Two"))
l.Add([]byte("Three"))

//3 is number of records & 0 is offset
resp, err := l.GetLast(3, 0)
```

### Log Iterator
```
iter, err := NewIterator(l)
//handle err
for i := 0; i < 10; i++ {
	val := string(iter.Value)
	fmt.Println(val)
	iter.Next()
}
```