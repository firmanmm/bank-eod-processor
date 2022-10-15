package pipeline

// BenefitCalculator represent pipeline stage to
// compute given benefit to user based on current balanced.
type BenefitCalculator struct {
	*WorkerPool
	next chan<- *EODRowData
}

// NewBenefitCalculator will return a new BenefitCalculator.
func NewBenefitCalculator(next chan<- *EODRowData) *BenefitCalculator {
	calculator := &BenefitCalculator{
		next: next,
	}
	pool := NewWorkerPool(getOptimumParallelism(), calculator.Execute)
	calculator.WorkerPool = pool
	return calculator
}

// Execute will process current data in the pipeline stage.
// In this case will give benefit to current user given below condition :
// - Will set the free transfer to 5 if balanced is between 100 to 150
// - Will increase the balance by 25 if balanced is more than 150
func (b *BenefitCalculator) Execute(workerID int, data *EODRowData) {
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
