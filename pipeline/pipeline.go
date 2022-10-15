package pipeline

import "runtime"

// IPipeline represent interface for pipeline executor.
type IPipeline interface {
	// Channel return pipeline's channel to push the work.
	Channel() chan<- *EODRowData
}

// EODRowData represent row data that is used for pipeline execution on
// the EoD data.
type EODRowData struct {
	Index            int
	InputRow         []string
	OutputRow        []string
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

// getOptimumParallelism will return value that is optimum for the worker pool (assuming for CPU intensive operation).
// Will always return 4 when number of CPU is lower than 4 to provide concurrency.
func getOptimumParallelism() int {
	cpu := runtime.NumCPU()
	// 4 is chosen to simulate concurrency when CPU is less than 4
	if cpu < 4 {
		cpu = 4
	}
	return cpu
}
