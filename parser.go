package bankeodprocessor

import (
	"runtime"
	"strconv"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

type Parser struct {
	*pipeline.WorkerPool
	next chan<- *pipeline.EODRowData
}

func NewParser(next chan<- *pipeline.EODRowData) *Parser {
	parser := &Parser{
		next: next,
	}
	pool := pipeline.NewWorkerPool(runtime.NumCPU(), parser.Execute)
	parser.WorkerPool = pool
	return parser
}

func (p *Parser) Execute(workerID int, data *pipeline.EODRowData) {
	inputRow := data.InputRow
	balanced, err := strconv.Atoi(inputRow[beforeEodHeaderIdxBalanced])
	if err != nil {
		data.Error = err
		data.FinishChannel <- data
		return
	}
	previousBalanced, err := strconv.Atoi(inputRow[beforeEodHeaderIdxPreviousBalanced])
	if err != nil {
		data.Error = err
		data.FinishChannel <- data
		return
	}
	freeTransfer, err := strconv.Atoi(inputRow[beforeEodHeaderIdxFreeTransfer])
	if err != nil {
		data.Error = err
		data.FinishChannel <- data
		return
	}
	averageBalance, err := strconv.Atoi(inputRow[beforeEodHeaderIdxAverageBalanced])
	if err != nil {
		data.Error = err
		data.FinishChannel <- data
		return
	}
	data.Balanced = balanced
	data.PreviousBalanced = previousBalanced
	data.FreeTransfer = freeTransfer
	data.AverageBalanced = averageBalance
	p.next <- data
}
