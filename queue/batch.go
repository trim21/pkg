package queue

import (
	"sync/atomic"
	"time"
)

// NewBatched create a batched queue, consume will receive a batched items on max size or on at timeout.
func NewBatched[T any](consume func([]T), size int, timeout time.Duration) *Batched[T] {
	return NewBatchedDedupe[T](consume, size, timeout, func(items []T) []T { return items })
}

// NewBatchedDedupe create a batched queue, with dedupe
func NewBatchedDedupe[T any](consume func([]T), size int, timeout time.Duration, dedupe func([]T) []T) *Batched[T] {
	if size == 0 {
		panic("size can't be 0")
	}

	if timeout == 0 {
		panic("timeout can't be 0")
	}

	if consume == nil {
		consume = func([]T) {}
	}

	q := &Batched[T]{
		batchSize:   size,
		consume:     consume,
		timeout:     timeout,
		c:           make(chan T, 1),
		closeSignal: make(chan struct{}),
		dedupe:      dedupe,
		flush:       make(chan struct{}, 1),
	}

	q.Start()

	return q
}

type Batched[T any] struct {
	closeSignal chan struct{}
	batchSize   int
	timeout     time.Duration
	c           chan T
	consume     func([]T)
	len         atomic.Int64
	dedupe      func([]T) []T
	flush       chan struct{}
}

func (q *Batched[T]) Start() {
	go q.background()
}

func (q *Batched[T]) Flush() {
	q.flush <- struct{}{}
}

func (q *Batched[T]) background() {
	queue := make([]T, 0, q.batchSize)

	var timeoutStart = time.Time{}

	// first timeout doesn't matter
	// when reach first delay without any items, this goroutine will sleep without any activate channel
	// when an item is pushed, timeoutStart will be set and delay will be set to correct value.
	delay := time.NewTimer(time.Hour)
	defer delay.Stop()

	var consume = func() {
		q.consume(queue)
		queue = queue[:0]
		q.len.Store(0)
	}

loop:
	for {
		t := q.timeout - time.Since(timeoutStart)
		if t > 0 {
			delay.Reset(t)
		}

		select {
		case item, ok := <-q.c:
			if !ok {
				// channel close
				break loop
			}

			queue = append(queue, item)

			queue = q.dedupe(queue)

			q.len.Store(int64(len(queue)))
			if len(queue) >= q.batchSize {
				consume()
			}

			if len(queue) == 1 {
				timeoutStart = time.Now()
			}
		case <-q.flush:
			if len(queue) > 0 {
				consume()
			}
		case <-delay.C:
			if len(queue) > 0 {
				consume()
			}
		}
	}

	if !delay.Stop() {
		<-delay.C
	}

	if len(queue) > 0 {
		consume()
	}

	q.closeSignal <- struct{}{}
}

func (q *Batched[T]) Push(item T) {
	q.c <- item
}

func (q *Batched[T]) Close() {
	close(q.c)
	<-q.closeSignal
}

func (q *Batched[T]) Len() int {
	return int(q.len.Load())
}
