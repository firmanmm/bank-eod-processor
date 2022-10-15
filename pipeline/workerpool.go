package pipeline

// WorkerPoolHandleFunc represent a worker pool implementation of goroutine.
type WorkerPoolHandleFunc func(workerID int, data *EODRowData)

// WorkerPool represent struct that manage worker pooling.
type WorkerPool struct {
	channel    chan *EODRowData
	handleFunc WorkerPoolHandleFunc
}

// NewWorkerPool return an implementation of worker pool given its parameters.
// The parallelism determine the amount of worker available to process given request.
func NewWorkerPool(parallelism int, handleFunc WorkerPoolHandleFunc) *WorkerPool {
	pool := &WorkerPool{
		channel:    make(chan *EODRowData, parallelism*3),
		handleFunc: handleFunc,
	}

	for i := 0; i < parallelism; i++ {
		go pool.routine(i + 1)
	}

	return pool
}

// Channel will return channel to push job into worker pool.
func (w *WorkerPool) Channel() chan<- *EODRowData {
	return w.channel
}

// routine represent internal routine to wait and execute
// new work given the set handler.
func (w *WorkerPool) routine(id int) {
	for work := range w.channel {
		if work == nil {
			return
		}
		w.handleFunc(id, work)
	}
}
