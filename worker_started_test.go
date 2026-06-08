package fluvio

import (
	"context"
	"sync"
	"testing"
)

type raceTestArgs struct{}

func (raceTestArgs) Kind() string { return "race-test" }

type raceTestWorker struct{}

func (raceTestWorker) Work(context.Context, *Job[raceTestArgs]) error { return nil }

func TestWorkersAddWorkerMarkStartedConcurrent(t *testing.T) {
	const iterations = 100

	for range iterations {
		w := NewWorkers()
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			defer func() {
				if r := recover(); r != nil {
					if r != "fluvio: AddWorker after Client.Start is not supported" {
						panic(r)
					}
				}
			}()
			AddWorker(w, raceTestWorker{})
		}()

		go func() {
			defer wg.Done()
			<-start
			w.markStarted()
		}()

		close(start)
		wg.Wait()
	}
}
