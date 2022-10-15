package pipeline

const (
	bonusDistributorRequiredParallelism = 8
)

// BonusDistributor represent a pipeline stage which will give
// bonus to the first 100 user in the input.
type BonusDistributor struct {
	*WorkerPool
	next chan<- *EODRowData
}

// NewBonusDistributor will return a new BonusDistributor.
func NewBonusDistributor(next chan<- *EODRowData) *BonusDistributor {
	distributor := &BonusDistributor{
		next: next,
	}
	pool := NewWorkerPool(bonusDistributorRequiredParallelism, distributor.Execute)
	distributor.WorkerPool = pool
	return distributor
}

// Execute will process current data in the pipeline stage.
// In this case will increase the balanced for the first 100 user in the pipeline.
func (a *BonusDistributor) Execute(workerID int, data *EODRowData) {
	if data.Index < 100 {
		data.ThreadNo3 = workerID
		data.Balanced += 10
	}
	if a.next != nil {
		a.next <- data
	} else {
		data.FinishChannel <- data
	}
}
