package main

import (
	"fmt"
	"sync"
	"time"
)

var jobCounter int

// A Job consists of an array of IDs (Targets) for which we want to gather data.
// ID is a unique id for each job in the current process
// Type conveys whether the Job is "lite" or "rich".
// Sent conveys whether or not the Job has been sent.
// Ack conveys whether or not the Job has been successfully completed.
type Job struct {
	ID      int
	Targets []int64
	Type    string
	Sent    bool
	Ack     bool
}

func process(sendChan chan Job, job Job) Job {
	job.Sent = true
	return job

}

func worker(wg *sync.WaitGroup, sendChan chan Job, receiveChan chan Job) {
	for job := range sendChan {
		output := process(sendChan, job)
		receiveChan <- output
	}
	wg.Done()
}

func createWorkerPool(noOfWorkers int, sendChan chan Job, receiveChan chan Job) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, sendChan, receiveChan)
	}
	wg.Wait()
	close(receiveChan)
}

func allocate(noOfJobs int, sendChan chan Job) {
	for i := 0; i < noOfJobs; i++ {
		job := Job{ID: jobCounter}
		jobCounter++
		sendChan <- job
	}
	close(sendChan)
}

func result(done chan bool, receiveChan chan Job) {
	for result := range receiveChan {
		fmt.Printf("Job id %v\n", result.ID)
	}
	done <- true
}
func acknowledge(done chan Job, receiveChan chan Job) {
	for result := range receiveChan {
		result.Ack = true
		fmt.Printf("Job id %v\n", result.ID)
		done <- result
	}

}

func main() {
	liteTargets := make([]int64, 200)
	sendMinnowChan := make(chan Job, 200)
	receiveMinnowChan := make(chan Job, 200)

	startTime := time.Now()
	go allocate(len(liteTargets), sendMinnowChan)
	done := make(chan Job)
	go acknowledge(done, receiveMinnowChan)
	noOfWorkers := 3
	createWorkerPool(noOfWorkers, sendMinnowChan, receiveMinnowChan)
	<-done
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}
