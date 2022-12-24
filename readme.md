## queue.Batched[T]

```go
func NewBatched[T any](consume func([]T), size int, timeout time.Duration) *Batched[T]
```

batched queue. elements will be consumed in batch,
and **any elements pushed to this queue will be consumed in timeout**.

Precision is limited by go timer, you should not use very short time like `1ms`, it may not work as you expected.

### batch

if you push 100 element with batch size 10, consume will be called 10 times, each with chunked 10 items.

### timeout

```golang
q := NewBatched(..., time.Second)

q.Push(1)
```

If no other element is pushed to queue in the second, item `1` will be consumed in 1s later after it's pushed.

And when a batch is consumed, timeout will be reset, and start counting until any new item pushed.
