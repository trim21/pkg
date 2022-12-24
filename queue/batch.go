package queue

import (
	"sync/atomic"
	"time"
)

// NewBatched create a batched queue, consume will receive a batched items on max size or on at timeout.
func NewBatched[T any](consume func([]T), size int, timeout time.Duration) *Batched[T] {
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
}

func (q *Batched[T]) Start() {
	go q.background()
}

func (q *Batched[T]) background() {
	queue := make([]T, 0, q.batchSize)

	var firstElementPush = time.Time{}

	delay := time.NewTimer(time.Hour) // first timeout doesn't matter, it's not actually used because it will be reset to q.timeout - firstPush
	defer delay.Stop()

	var consume = func() {
		q.consume(queue)
		queue = queue[:0]
		q.len.Store(0)
	}

loop:
	for {
		t := q.timeout - time.Since(firstElementPush)
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

			q.len.Store(int64(len(queue)))
			if len(queue) >= q.batchSize {
				consume()
			}

			if len(queue) == 1 {
				firstElementPush = time.Now()
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
