package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// NewBatched create a batched queue, consume will receive a batched items on max size or on at interval.
func NewBatched[T any](consume func([]T), size int, interval time.Duration) *Batched[T] {
	q := &Batched[T]{
		batchSize: size,
		consume:   consume,
		interval:  interval,
		queue:     make([]T, 0, size),
	}

	q.Start()

	return q
}

type Batched[T any] struct {
	mw        sync.Mutex
	closed    atomic.Bool
	queue     []T
	batchSize int
	interval  time.Duration
	consume   func([]T)
}

func (q *Batched[T]) Start() {
	go q.background()
}

func (q *Batched[T]) background() {
	for {
		if q.closed.Load() {
			break
		}
		time.Sleep(q.interval)
		q.mw.Lock()
		if len(q.queue) != 0 {
			q.consumeBatch()
		}
		q.mw.Unlock()
	}
}

func (q *Batched[T]) Push(item T) {
	if q.closed.Load() {
		panic("push item to a closed queue")
	}
	q.mw.Lock()
	q.queue = append(q.queue, item)
	if len(q.queue) >= q.batchSize {
		q.consumeBatch()
	}
	q.mw.Unlock()
}

func (q *Batched[T]) Close() {
	q.mw.Lock()
	defer q.mw.Unlock()
	if len(q.queue) != 0 {
		q.consume(q.queue)
	}
	q.closed.Store(true)
}

func (q *Batched[T]) consumeBatch() {
	queue := q.queue
	q.queue = make([]T, 0, q.batchSize)
	q.consume(queue)
}

func (q *Batched[T]) Len() int {
	q.mw.Lock()
	l := len(q.queue)
	q.mw.Unlock()

	return l
}
