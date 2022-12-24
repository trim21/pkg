package queue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/trim21/pkg/queue"
)

// basic batch
func TestBatched_1(t *testing.T) {
	defer goleak.VerifyNone(t)
	var actual [][]int
	q := queue.NewBatched[int](func(batch []int) {
		var input = make([]int, len(batch))
		copy(input, batch)
		actual = append(actual, input)
	}, 10, time.Second)

	// 	{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	// 	{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	for i := 1; i < 23; i++ {
		q.Push(i)
	}

	q.Close()

	require.Equal(t,
		[][]int{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			{21, 22},
		},
		actual)
}

// test batch timeout
func TestBatched_2(t *testing.T) {
	defer goleak.VerifyNone(t)
	var actual [][]int
	q := queue.NewBatched[int](func(batch []int) {
		var input = make([]int, len(batch))
		copy(input, batch)
		actual = append(actual, input)
	}, 10, time.Second)

	q.Push(100)

	time.Sleep(time.Millisecond * 700)
	// 0.7s
	q.Push(101)
	q.Push(102)

	time.Sleep(time.Millisecond * 700)
	// 1.7s

	q.Push(103)

	time.Sleep(time.Second * 2)
	// 3.7s

	for i := 0; i < 8; i++ {
		q.Push(i)
	}

	q.Close()

	require.Equal(t,
		[][]int{
			{100, 101, 102},
			{103},
			{0, 1, 2, 3, 4, 5, 6, 7},
		},
		actual)
}
