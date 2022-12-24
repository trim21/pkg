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

	time.Sleep(time.Second * 2)
	// 2s

	time.Sleep(time.Millisecond * 700) // 2.7s

	q.Push(101) // first element, should reset waiting timeout to 0s

	time.Sleep(time.Millisecond * 700) // 0.7s

	q.Push(102)

	time.Sleep(time.Millisecond * 700) // 1.4s, should consume

	q.Push(103) // 0s
	q.Push(104) // 0s

	time.Sleep(time.Second * 2) // 2s should consume

	for i := 0; i < 8; i++ {
		q.Push(i)
	}

	q.Close()

	require.Equal(t,
		[][]int{
			{100},
			{101, 102},
			{103, 104},
			{0, 1, 2, 3, 4, 5, 6, 7},
		},
		actual)
}

// basic batch
func TestBatched_dedupe(t *testing.T) {
	defer goleak.VerifyNone(t)
	var actual [][]int
	q := queue.NewBatchedDedupe[int](func(batch []int) {
		var input = make([]int, len(batch))
		copy(input, batch)
		actual = append(actual, input)
	}, 10, time.Hour, func(items []int) []int {
		keys := make(map[int]bool)
		var list = make([]int, 0, len(items))

		for _, entry := range items {
			if _, value := keys[entry]; !value {
				keys[entry] = true
				list = append(list, entry)
			}
		}
		return list
	})

	for i := 1; i < 9; i++ {
		q.Push(i)
	}

	for i := 0; i < 9; i++ {
		q.Push(9)
	}

	q.Push(10)

	q.Close()

	require.Equal(t,
		[][]int{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		actual)
}
