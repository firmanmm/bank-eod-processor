package pipeline

type AverageCalculator struct {
	*WorkerPool
	next chan<- *EODRowData
}

func NewAverageCalculator(next chan<- *EODRowData) *AverageCalculator {
	calculator := &AverageCalculator{
		next: next,
	}
	pool := NewWorkerPool(getOptimumParallelism(), calculator.Execute)
	calculator.WorkerPool = pool
	return calculator
}

func (a *AverageCalculator) Execute(workerID int, data *EODRowData) {
	data.ThreadNo1 = workerID
	data.AverageBalanced = (data.PreviousBalanced + data.Balanced) / 2
	if a.next != nil {
		a.next <- data
	} else {
		data.FinishChannel <- data
	}
}
