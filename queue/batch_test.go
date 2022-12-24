package queue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/trim21/go-pkg/queue"
)

func TestBatched(t *testing.T) {
	defer goleak.VerifyNone(t)
	var m sync.Mutex
	var actual [][]int
	q := queue.NewBatched[int](func(batch []int) {
		m.Lock()
		actual = append(actual, batch)
		m.Unlock()
	}, 10, time.Second*2)

	for i := 0; i < 22; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 3)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 3)

	for i := 0; i < 8; i++ {
		q.Push(i)
	}

	q.Close()

	require.Equal(t,
		[][]int{
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			{20, 21},
			{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			{0, 1, 2, 3, 4, 5, 6, 7},
		},
		actual)

	time.Sleep(time.Second * 3)
}
