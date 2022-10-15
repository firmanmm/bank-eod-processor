package pipeline

// AverageCalculator represent pipeline stage that perform
// calculation of average of previous balanced and current balanced.
type AverageCalculator struct {
	*WorkerPool
	next chan<- *EODRowData
}

// NewAverageCalculator return a new AverageCalculator.
func NewAverageCalculator(next chan<- *EODRowData) *AverageCalculator {
	calculator := &AverageCalculator{
		next: next,
	}
	pool := NewWorkerPool(getOptimumParallelism(), calculator.Execute)
	calculator.WorkerPool = pool
	return calculator
}

// Execute will process current data in the pipeline stage.
// In this case will average previous balanced and current balanced.
func (a *AverageCalculator) Execute(workerID int, data *EODRowData) {
	data.ThreadNo1 = workerID
	data.AverageBalanced = (data.PreviousBalanced + data.Balanced) / 2
	if a.next != nil {
		a.next <- data
	} else {
		data.FinishChannel <- data
	}
}
