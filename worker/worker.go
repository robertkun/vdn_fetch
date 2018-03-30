package worker

import (
	"log"
	"../com"
	"../handle"
)

type Worker struct {
	ID      int
	RepJobs chan *com.JobInfo
	quit    chan bool
}

type WorkerPool struct {
	workerChan chan *Worker
	workerList []*Worker
}

var jobQueue = make(chan *com.JobInfo, 10)
var workerPool = new(WorkerPool)

func NewWorker(i int) *Worker {
	var worker = new(Worker)
	worker.ID = i
	worker.RepJobs = make(chan *com.JobInfo)
	worker.quit = make(chan bool)
	return worker
}

func InitWorkerPool() error {
	n := 10
	workerPool = &WorkerPool{
		workerChan: make(chan *Worker, n),
		workerList: make([]*Worker, 0, n),
	}

	for i := 1; i <= n; i++ {
		worker := NewWorker(i)
		workerPool.workerList = append(workerPool.workerList, worker)
		worker.Start()
		log.Printf("worker %d started.\n", worker.ID)
	}

	return nil
}

func (w *Worker) Start() {
	go func() {
		for {
			workerPool.workerChan <- w

			select {
			case job := <-w.RepJobs:
				w.handleRepJob(job)
			case q := <-w.quit:
				if q {
					log.Printf("worker: %d, will stop.", w.ID)
					return
				}
			}
		}
	}()
}

func JobDispatch() {
	go func() {
		for {
			select {
			case job := <-jobQueue:
				go func(job *com.JobInfo) {
					worker := <-workerPool.workerChan
					worker.RepJobs <- job
				}(job)
			}
		}
	}()
}

func (w *Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func AddJob(job *com.JobInfo) {
	jobQueue <- job
}

func (w *Worker) handleRepJob(job *com.JobInfo) {
	//log.Println("Start Handle Job:", job.JobId)

	handle.ProcessFlowV5(job)
	handle.ProcessFlowS5(job)
	handle.ProcessPIspFlowV5(job)

	handle.ProcessPvV5(job)
	handle.ProcessPvS5(job)

	handle.ProcessRescodeV5(job)
	handle.ProcessRescodeS5(job)

	handle.ProcessDetail(job)

	log.Printf("Job[%v] over! customer[%v], domain[%v], tm[%v].", job.JobId, job.CuId, job.DmId, job.Tm)
}