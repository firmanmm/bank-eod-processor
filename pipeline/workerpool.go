package pipeline

type WorkerPoolHandleFunc func(workerID int, data *EODRowData)

type WorkerPool struct {
	channel    chan *EODRowData
	handleFunc WorkerPoolHandleFunc
}

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

func (w *WorkerPool) Channel() chan<- *EODRowData {
	return w.channel
}

func (w *WorkerPool) routine(id int) {
	for work := range w.channel {
		if work == nil {
			return
		}
		w.handleFunc(id, work)
	}
}
