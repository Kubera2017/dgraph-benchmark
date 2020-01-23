package main

import (
	"fmt"
	"time"
)

func manager(chM chan bool, jobRequestsCh chan int, dataChs [workersCount]chan Batch) {
	entriesCounter := 0

	statusCounter := 0
	const statusAt = 10000
	tStart := time.Now()
	tMiddleStart := tStart

	for workerID := range jobRequestsCh {
		if entriesCounter >= maxEntries {
			break
		}
		batch := Batch{
			entries: make([]Entry, batchSize),
		}
		for j := 0; j < batchSize; j++ {
			batch.entries = append(batch.entries, generateEntry())
		}
		dataChs[workerID] <- batch
		entriesCounter += batchSize
		if entriesCounter-statusCounter*statusAt > statusAt {
			tMiddleEnd := time.Now()
			statusCounter++
			fmt.Println("Proceeded", statusCounter*statusAt, tMiddleEnd.Sub(tMiddleStart))
			tMiddleStart = tMiddleEnd
		}
	}
	close(jobRequestsCh)
	for i := range dataChs {
		close(dataChs[i])
	}
	chM <- true
}
