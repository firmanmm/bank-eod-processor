package pipeline

const (
	bonusDistributorRequiredParallelism = 8
)

type BonusDistributor struct {
	*WorkerPool
	next chan<- *EODRowData
}

func NewBonusDistributor(next chan<- *EODRowData) *BonusDistributor {
	distributor := &BonusDistributor{
		next: next,
	}
	pool := NewWorkerPool(bonusDistributorRequiredParallelism, distributor.Execute)
	distributor.WorkerPool = pool
	return distributor
}

func (a *BonusDistributor) Execute(workerID int, data *EODRowData) {
	data.ThreadNo3 = workerID
	data.AverageBalanced = (data.PreviousBalanced + data.Balanced) / 2
	if a.next != nil {
		a.next <- data
	} else {
		data.FinishChannel <- data
	}
}
