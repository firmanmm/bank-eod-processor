package pipeline

import "runtime"

type IPipeline interface {
	Channel() chan<- *EODRowData
}

type EODRowData struct {
	InputRow         []string
	OuputRow         []string
	AverageBalanced  int
	PreviousBalanced int
	Balanced         int
	FreeTransfer     int
	ThreadNo1        int
	ThreadNo2A       int
	ThreadNo2B       int
	ThreadNo3        int

	FinishChannel chan<- *EODRowData
	Error         error
}

func getOptimumParallelism() int {
	cpu := runtime.NumCPU()
	// 4 is chosen to simulate concurrency when CPU is less than 4
	if cpu < 4 {
		cpu = 4
	}
	return cpu
}
