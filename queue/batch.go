package queue

import (
	"sync"
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
		c:           make(chan T),
		closeSignal: make(chan struct{}),
	}

	q.Start()

	return q
}

type Batched[T any] struct {
	m           sync.Mutex
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
	q.m.Lock()
	defer q.m.Unlock()
	queue := make([]T, 0, q.batchSize)

	var lastConsume = time.Now()

	delay := time.NewTimer(q.timeout)
	defer delay.Stop()

	var consume = func() {
		q.consume(queue)
		queue = queue[:0]
		q.len.Store(0)
		lastConsume = time.Now()
	}

loop:
	for {
		delay.Reset(q.timeout - time.Since(lastConsume))

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
		case <-delay.C:
			if len(queue) > 0 {
				consume()
			} else {
				lastConsume = time.Now()
			}

		case <-q.closeSignal:
			break loop
		}
	}

	if len(queue) > 0 {
		consume()
	}
}

func (q *Batched[T]) Push(item T) {
	q.c <- item
}

func (q *Batched[T]) Close() {
	q.closeSignal <- struct{}{}
	close(q.c)
	q.m.Lock()
	q.m.Unlock()
}

func (q *Batched[T]) Len() int {
	return int(q.len.Load())
}
