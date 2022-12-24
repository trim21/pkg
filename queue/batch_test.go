package queue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/trim21/pkg/queue"
)

func TestBatched(t *testing.T) {
	defer goleak.VerifyNone(t)
	var actual [][]int
	q := queue.NewBatched[int](func(batch []int) {
		var input = make([]int, len(batch))
		copy(input, batch)
		actual = append(actual, input)
	}, 10, time.Second)

	for i := 1; i < 23; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 3)

	for i := -3; i < 7; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 3)

	for i := 0; i < 8; i++ {
		q.Push(i)
	}

	q.Close()

	require.Equal(t,
		[][]int{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			{21, 22},
			{-3, -2, -1, 0, 1, 2, 3, 4, 5, 6},
			{0, 1, 2, 3, 4, 5, 6, 7},
		},
		actual)
}
