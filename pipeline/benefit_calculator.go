package pipeline

type BenefittCalculator struct {
	*WorkerPool
	next chan<- *EODRowData
}

func NewBenefitCalculator(next chan<- *EODRowData) *BenefittCalculator {
	calculator := &BenefittCalculator{
		next: next,
	}
	pool := NewWorkerPool(getOptimumParallelism(), calculator.Execute)
	calculator.WorkerPool = pool
	return calculator
}

func (b *BenefittCalculator) Execute(workerID int, data *EODRowData) {
	if data.Balanced >= 100 && data.Balanced <= 150 {
		data.ThreadNo2A = workerID
		data.FreeTransfer = 5
	} else if data.Balanced > 150 {
		data.ThreadNo2B = workerID
		data.Balanced += 25
	}

	if b.next != nil {
		b.next <- data
	} else {
		data.FinishChannel <- data
	}
}
