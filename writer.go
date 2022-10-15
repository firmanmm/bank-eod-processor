package bankeodprocessor

import (
	"runtime"
	"strconv"
	"sync"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

type Writer struct {
	*pipeline.WorkerPool

	waitGroup *sync.WaitGroup
}

func NewWriter(waitGroup *sync.WaitGroup) *Writer {
	writer := &Writer{
		waitGroup: waitGroup,
	}
	pool := pipeline.NewWorkerPool(runtime.NumCPU(), writer.Execute)
	writer.WorkerPool = pool
	return writer
}

func (w *Writer) Execute(workerID int, data *pipeline.EODRowData) {
	data.FinishChannel = nil
	if data.Error != nil {
		errorIdx := 0
		if data.ThreadNo1 == 0 {
			errorIdx = int(afterEodHeaderIdxNo1Thread)
		} else if data.ThreadNo2A == 0 {
			errorIdx = int(afterEodHeaderIdxNo2AThread)
		} else if data.ThreadNo2B == 0 {
			errorIdx = int(afterEodHeaderIdxNo2BThread)
		} else if data.ThreadNo3 == 0 {
			errorIdx = int(afterEodHeaderIdxNo3Thread)
		}
		data.OuputRow[errorIdx] = data.Error.Error()
	} else {
		outputRow := data.OuputRow
		outputRow[afterEodHeaderIdxBalanced] = strconv.Itoa(data.Balanced)
		outputRow[afterEodHeaderIdxAverageBalanced] = strconv.Itoa(data.AverageBalanced)
		outputRow[afterEodHeaderIdxFreeTransfer] = strconv.Itoa(data.FreeTransfer)
		outputRow[afterEodHeaderIdxNo1Thread] = strconv.Itoa(data.ThreadNo1)
		outputRow[afterEodHeaderIdxNo2AThread] = strconv.Itoa(data.ThreadNo2A)
		outputRow[afterEodHeaderIdxNo2BThread] = strconv.Itoa(data.ThreadNo2B)
		outputRow[afterEodHeaderIdxNo3Thread] = strconv.Itoa(data.ThreadNo3)
	}
	w.waitGroup.Done()
}
