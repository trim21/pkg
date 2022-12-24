package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// NewBatched create a batched queue, consume will receive a batched items on max size or on at timeout.
func NewBatched[T any](consume func([]T), size int, timeout time.Duration) *Batched[T] {
	ctx, canal := context.WithCancel(context.Background())

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
		batchSize: size,
		consume:   consume,
		timeout:   timeout,
		c:         make(chan T),
		canal:     canal,
		ctx:       ctx,
	}

	q.Start()

	return q
}

type Batched[T any] struct {
	m         sync.Mutex
	closed    atomic.Bool
	ctx       context.Context
	canal     func()
	batchSize int
	timeout   time.Duration
	c         chan T
	consume   func([]T)
	len       atomic.Int64
}

func (q *Batched[T]) Start() {
	go q.background()
}

func (q *Batched[T]) background() {
	q.m.Lock()
	defer q.m.Unlock()
	queue := make([]T, 0, q.batchSize)

	delay := time.NewTimer(q.timeout)
	defer delay.Stop()

loop:
	for {
		delay.Reset(q.timeout)

		select {
		case item, ok := <-q.c:
			if !ok {
				// channel close
				break loop
			}

			queue = append(queue, item)

			q.len.Store(int64(len(queue)))
			if len(queue) >= q.batchSize {
				q.consume(queue)
				queue = queue[:0]
				q.len.Store(0)
			}

		case <-delay.C:
			if len(queue) > 0 {
				q.consume(queue)
				queue = queue[:0]
				q.len.Store(0)
			}

		case <-q.ctx.Done():
			break loop
		}
	}

	if len(queue) > 0 {
		q.consume(queue)
	}
}

func (q *Batched[T]) Push(item T) {
	if q.closed.Load() {
		panic("push item to a closed queue")
	}
	q.c <- item
}

func (q *Batched[T]) Close() {
	q.closed.Store(true)
	q.canal()
	close(q.c)
	q.m.Lock()
	q.m.Unlock()
}

func (q *Batched[T]) Len() int {
	return int(q.len.Load())
}
