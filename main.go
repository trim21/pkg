package main

import (
	"fmt"
	"time"

	"github.com/trim21/pkg/queue"
)

func main() {
	q := queue.NewBatched[int](func(batch []int) { fmt.Println(batch) }, 20, time.Second*10)

	for i := 0; i < 110; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 20)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	time.Sleep(time.Second * 20)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	q.Close()
}
