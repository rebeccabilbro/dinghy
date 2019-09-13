package main

import (
	"fmt"
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
	Worker  int
}

func process(sendChan chan Job, job Job, worker int) Job {
	job.Sent = true
	job.Worker = worker
	return job

}

func worker(ID int, sendChan chan Job, receiveChan chan Job) {
	for job := range sendChan {
		fmt.Printf("\nWorker %v is working on job %v", ID, job)
		output := process(sendChan, job, ID)
		receiveChan <- output
		fmt.Printf("\nWorker %v completed job %v", ID, output)

	}
}

func main() {
	numWorkers := 10                             // the number of workers processing jobs
	jobLen := int(300)                           // do 300 jobs at a time
	liteTargets := make([]Job, 1000000)          // there are 1000000 total
	nextTime := time.Now().Truncate(time.Minute) // make a counter

	// loop over all jobs, 300 at a time
	for start := 0; start < len(liteTargets); start += jobLen {
		for {
			stop := start + jobLen

			if len(liteTargets) < stop {
				stop = len(liteTargets)
			}
			if stop == start {
				fmt.Println("\nAll done!")
				return
			}
			loopTargets := liteTargets[start:stop]
			start += jobLen

			fmt.Printf("\n\n\nSending %v minnow jobs", jobLen)
			minnowJobs := make(chan Job, jobLen)
			minnowResults := make(chan Job, jobLen)

			for x := 1; x <= numWorkers; x++ {
				go worker(x, minnowJobs, minnowResults)
			}

			// make jobs
			for _, j := range loopTargets {
				j.ID = jobCounter
				minnowJobs <- j
				jobCounter++
			}

			close(minnowJobs)

			for r := 1; r <= len(loopTargets); r++ {
				job := <-minnowResults
				job.Ack = true
				fmt.Printf("\nJob received from worker: %v", job)
			}
			close(minnowResults)

			// wait
			nextTime = nextTime.Add(time.Second * 15)
			time.Sleep(time.Until(nextTime))
		}
	}
}
