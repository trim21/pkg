package main

import (
	"fmt"
	"time"

	"github.com/trim21/pkg/queue"
)

func main() {
	for {
		q := queue.NewBatched[int](func(batch []int) {
			fmt.Println(batch)
		}, 10, time.Second)

		for i := 0; i < 22; i++ {
			q.Push(i)
		}

		q.Close()

		time.Sleep(time.Second)
	}
}
