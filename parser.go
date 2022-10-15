package bankeodprocessor

import (
	"runtime"
	"strconv"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

// Parser represent CSV Parser pipeline for EOD operation.
// Will read from input row in the pipeline and write it as parsed value.
// Will return error and terminate pipeline for current flow if encounter error.
type Parser struct {
	*pipeline.WorkerPool
	next chan<- *pipeline.EODRowData
}

// NewParser will return a new Parser.
func NewParser(next chan<- *pipeline.EODRowData) *Parser {
	parser := &Parser{
		next: next,
	}
	pool := pipeline.NewWorkerPool(runtime.NumCPU(), parser.Execute)
	parser.WorkerPool = pool
	return parser
}

// Execute will process current data in the pipeline stage.
// In this case will parse and set the parsed data into the pipeline for further
// operation
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
