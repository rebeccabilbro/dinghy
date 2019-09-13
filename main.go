package main

import (
	"fmt"
	"time"
)

var liteJobCounter int
var richJobCounter int

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
	numWorkers := 20       // the number of workers processing jobs
	liteJobLen := int(300) // how many lite jobs to do at a time
	// richJobLen := int(15)                        // how many rich jobs to do at a time
	liteTargets := make([]Job, 100000) // just get high level details
	// richTargets := make([]Job, 1000000)          // get more user details
	nextTime := time.Now().Truncate(time.Minute) // keep track of time

	// loop over all jobs, 300 at a time
	for liteStart := 0; liteStart < len(liteTargets); liteStart += liteJobLen {
		for {
			liteStop := liteStart + liteJobLen

			if len(liteTargets) < liteStop {
				liteStop = len(liteTargets)
			}
			if liteStop == liteStart {
				fmt.Println("\nAll done!")
				return
			}
			liteLoopTargets := liteTargets[liteStart:liteStop]
			liteStart += liteJobLen

			fmt.Printf("\n\n\nSending %v minnow jobs", liteJobLen)
			minnowJobs := make(chan Job, liteJobLen)
			minnowResults := make(chan Job, liteJobLen)

			for x := 1; x <= numWorkers; x++ {
				go worker(x, minnowJobs, minnowResults)
			}

			// make jobs
			for _, j := range liteLoopTargets {
				j.ID = liteJobCounter
				j.Type = "lite"
				minnowJobs <- j
				liteJobCounter++
			}

			close(minnowJobs)

			for r := 1; r <= len(liteLoopTargets); r++ {
				job := <-minnowResults
				job.Ack = true
				fmt.Printf("\nJob received from worker: %v", job)
			}
			close(minnowResults)

			// loop over rich jobs

			// wait
			nextTime = nextTime.Add(time.Second * 15)
			time.Sleep(time.Until(nextTime))
		}
	}
}
