package main

import (
	"fmt"
	"time"
)

var liteJobCounter int
var richJobCounter int

// A Job consists of a target for which we want to gather data.
type Job struct {
	Target   int64  // the target of the job
	ID       int    // the ID of the job
	Type     string // the type of job
	Sent     bool   // whether the job has been sent on the send channel
	Ack      bool   // whether the job has been acknowledged by the receive channel
	WorkerID int    // the worker who processed the job
}

func process(sendChan chan Job, job Job, worker int) Job {
	job.Sent = true
	job.WorkerID = worker
	return job

}

func handle(ID int, sendChan chan Job, receiveChan chan Job) {
	for job := range sendChan {
		fmt.Printf("\n %v worker %v is working on job %v", job.Type, ID, job.ID)
		output := process(sendChan, job, ID)
		receiveChan <- output
		fmt.Printf("\n %v worker %v completed job %v", job.Type, ID, job.ID)

	}
}

func main() {
	pause := time.Second * 15                    // The amount of time to wait between cycles
	numLiteWorkers := 20                         // the number of workers processing lite jobs
	numRichWorkers := 3                          // the number of workers processing rich jobs
	liteJobLen := int(300)                       // how many lite jobs to do at a time
	richJobLen := int(15)                        // how many rich jobs to do at a time
	liteTargets := make([]Job, 100000)           // just get high level details
	richTargets := make([]Job, 100000)           // get more user details
	nextTime := time.Now().Truncate(time.Minute) // keep track of time

	// loop over all lite jobs, `liteJobLen` at a time
	for liteStart := 0; liteStart < len(liteTargets); liteStart += liteJobLen {
		for {
			// We want to subslice our lite targets, starting at whereever we left off
			// in the last loop and stopping after `liteJobLen`
			liteStop := liteStart + liteJobLen

			// However, if we're getting close to the end of the list, we'll stop at the end instead
			if len(liteTargets) < liteStop {
				liteStop = len(liteTargets)
			}

			// And if we're at the end, we're done!
			if liteStop == liteStart {
				fmt.Println("\nAll done!")
				return
			}

			// Otherwise subslice the lite targets for this loop
			liteLoopTargets := liteTargets[liteStart:liteStop]

			// Increment the start for the next loop
			liteStart += liteJobLen

			fmt.Printf("\n\n\nSending %v lite jobs", liteJobLen)

			// Create buffered channels for the jobs and the results
			// We want the buffer to be exactly as long as the expected jobs & results
			minnowJobs := make(chan Job, liteJobLen)
			minnowResults := make(chan Job, liteJobLen)

			// For each worker we've assigned to handle lite jobs, pass them the work
			// as a goroutine so that they can handle it concurrently.
			// The workers know to read jobs from the minnowJobs channel, process the jobs,
			// and acknowledge their results via the minnowResults channel.
			for liteWorker := 1; liteWorker <= numLiteWorkers; liteWorker++ {
				go handle(liteWorker, minnowJobs, minnowResults)
			}

			// Make lite jobs and put them into the send channel
			for _, j := range liteLoopTargets {
				j.ID = liteJobCounter
				j.Type = "lite"
				minnowJobs <- j
				liteJobCounter++
			}

			// When all the jobs that were put into the channel have finished, close it
			close(minnowJobs)

			// Now we'll read off the results channel, and close it once we're finished reading
			for r := 1; r <= len(liteLoopTargets); r++ {
				job := <-minnowResults
				job.Ack = true
				fmt.Printf("\nLite job received from worker: %v", job)
			}
			close(minnowResults)

			// Now we'll loop over the rich jobs
			richLoopTargets := richTargets[0:richJobLen]
			fmt.Printf("\n\n\nSending %v rich jobs", len(richLoopTargets))
			barracudaJobs := make(chan Job, len(richLoopTargets))
			barracudaResults := make(chan Job, len(richLoopTargets))

			// Loop over the available richWorkers
			for richWorker := 1; richWorker <= numRichWorkers; richWorker++ {
				go handle(richWorker, barracudaJobs, barracudaResults)
			}

			// Make jobs and send them to the workers on the barracuda jobs channel,
			// closing that channel once all the work has been delegated.
			for _, j := range richLoopTargets {
				j.ID = richJobCounter
				j.Type = "rich"
				barracudaJobs <- j
				richJobCounter++
			}
			close(barracudaJobs)

			// Read over the results and then close that channel
			for r := 1; r <= len(richLoopTargets); r++ {
				job := <-barracudaResults
				job.Ack = true
				fmt.Printf("\nRich job received from worker: %v", job)
			}
			close(barracudaResults)

			// Truncate the rich targets
			richTargets = richTargets[richJobLen:]

			// Pause the loop to avoid exceeding the rate limit
			nextTime = nextTime.Add(pause)
			fmt.Printf("\nGoing to sleep until %v", nextTime)
			time.Sleep(time.Until(nextTime))
		}
	}
}
