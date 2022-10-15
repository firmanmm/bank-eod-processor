package bankeodprocessor

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

type (
	CSVHeaderIndex       int
	CSVHeaderInputIndex  CSVHeaderIndex
	CSVHeaderOutputIndex CSVHeaderIndex
)

const (
	beforeEodHeaderIdxID CSVHeaderInputIndex = iota
	beforeEodHeaderIdxNama
	beforeEodHeaderIdxAge
	beforeEodHeaderIdxBalanced
	beforeEodHeaderIdxPreviousBalanced
	beforeEodHeaderIdxAverageBalanced
	beforeEodHeaderIdxFreeTransfer
)
const (
	afterEodHeaderIdxID CSVHeaderOutputIndex = iota
	afterEodHeaderIdxNama
	afterEodHeaderIdxAge
	afterEodHeaderIdxBalanced
	afterEodHeaderIdxNo2BThread
	afterEodHeaderIdxNo3Thread
	afterEodHeaderIdxPreviousBalanced
	afterEodHeaderIdxAverageBalanced
	afterEodHeaderIdxNo1Thread
	afterEodHeaderIdxFreeTransfer
	afterEodHeaderIdxNo2AThread
)

var (
	beforeEodCSVHeader = []string{
		"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer",
	}
	afterEodCSVHeader = []string{
		"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No",
	}

	ErrInvalidInputRows  = errors.New("invalid input rows provided")
	ErrInvalidOutputRows = errors.New("invalid output rows provided")
	ErrInvalidHeader     = errors.New("invalid header provided")
)

// EODProcessor represent struct can process EOD operation.
type EODProcessor struct {
	pipeline pipeline.IPipeline
}

// NewEODProcessor will return a new EODProcessor to process data given it's pipeline executor.
func NewEODProcessor(pipeline pipeline.IPipeline) *EODProcessor {
	return &EODProcessor{
		pipeline: pipeline,
	}
}

// Process will process from given input and output file name.
// Will also write the result on the output file.
func (e *EODProcessor) Process(ctx context.Context, inputFileName, outputFileName string) error {
	result, err := e.ProcessFile(ctx, inputFileName, outputFileName)
	if err != nil {
		return err
	}
	fileHandle, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf(`failed to write to provided output file %w`, err)
	}
	writer := csv.NewWriter(fileHandle)
	writer.Comma = ';'
	return writer.WriteAll(result)
}

// ProcessFile will read from given input file name and output template file name.
// Will read the input and output as CSV file.
// If output file is not found then it will assume that the template is empty will treat it as empty slice.
// Will return slice resulted from the operation that can be treated as CSV.
// Will return nil slice and an error on fail.
func (e *EODProcessor) ProcessFile(ctx context.Context, inputFileName, outputTemplateFileName string) ([][]string, error) {
	inputHandle, err := os.Open(inputFileName)
	if err != nil {
		return nil, fmt.Errorf(`failed to process provided input file %w`, err)
	}
	defer inputHandle.Close()
	// Make sure input is valid so we won't waste unnecessary read on output file.
	reader := csv.NewReader(inputHandle)
	reader.Comma = ';'
	inputRows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf(`failed to process provided input file %w`, err)
	}
	outputHandle, err := os.Open(outputTemplateFileName)
	var outputRows [][]string
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		// Handle case where the output file is not found.
		// In this case we treat it as empty rows.
		// +1 capacity since the first row is always header to avoid resize.
		outputRows = make([][]string, 1, len(inputRows)+1)
		outputRows[0] = afterEodCSVHeader
	} else {
		// Handle case where the output file is provided.
		// It will instead use that file as read source template to maintain ordering.
		defer outputHandle.Close()
		reader = csv.NewReader(outputHandle)
		reader.Comma = ';'
		outputRows, err = reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf(`failed to process provided output file %w`, err)
		}
	}
	return e.ProcessSlice(ctx, inputRows, outputRows)
}

// ProcessSlice will process given slices that can be treated as CSV given it's input and output rows.
// Will return updated output rows with any addition if necessary.
// Will return nil slice and an error on fail.
func (e *EODProcessor) ProcessSlice(ctx context.Context, inputRows, outputRows [][]string) ([][]string, error) {
	outputIDMap, outputRows, err := e.preProcessRows(ctx, inputRows, outputRows)
	if err != nil {
		return nil, err
	}
	waitGroup := &sync.WaitGroup{}
	// Adjustment for headers
	writer := NewWriter(waitGroup)
	waitGroup.Add(len(inputRows) - 1)
	channel := e.pipeline.Channel()
	for idx, row := range inputRows[1:] {
		channel <- &pipeline.EODRowData{
			Index:         idx,
			InputRow:      row,
			OutputRow:     outputRows[outputIDMap[row[0]]],
			FinishChannel: writer.Channel(),
		}
	}
	waitGroup.Wait()
	return outputRows, nil
}

// preProcessRows will perform rows preprocessing to fill missing output before being processed.
// Will also peform validation to prevent executing on bad data.
// Will return map indicating the id to row index and updated output rows on fixed data.
// Will return nil map and slice and an error on fail.
func (e *EODProcessor) preProcessRows(ctx context.Context, inputRows, outputRows [][]string) (map[string]int, [][]string, error) {
	// Validate rows length
	if len(inputRows) == 0 {
		return nil, nil, ErrInvalidInputRows
	}
	if len(outputRows) == 0 {
		return nil, nil, ErrInvalidOutputRows
	}

	// Validate headers
	if err := e.validateHeaders(beforeEodCSVHeader, inputRows[0]); err != nil {
		return nil, nil, fmt.Errorf("failed to validate input header, %w", err)
	}

	if err := e.validateHeaders(afterEodCSVHeader, outputRows[0]); err != nil {
		return nil, nil, fmt.Errorf("failed to validate output header, %w", err)
	}

	maxCapacity := len(inputRows)
	outputLen := len(outputRows)
	if outputLen > maxCapacity {
		maxCapacity = outputLen
	}
	// Best assumed max capacity hint. Handle case where all id on
	// output rows is available in inputRows.
	outputIDRowMap := make(map[string]int, maxCapacity)
	for idx, row := range outputRows[1:] {
		// Plus 1 since we are skipping header
		outputIDRowMap[row[afterEodHeaderIdxID]] = idx + 1
	}

	// Fill missing rows in output target.
	// This is performed early instead of dynamic append
	// on finish so it can be lock free operation.
	outputRowLastIndex := len(outputRows)
	for _, row := range inputRows[1:] {
		rowID := row[beforeEodHeaderIdxID]
		if _, exist := outputIDRowMap[rowID]; !exist {
			// Fill missing data on output row
			outputRows = append(outputRows, []string{
				row[beforeEodHeaderIdxID],
				row[beforeEodHeaderIdxNama],
				row[beforeEodHeaderIdxAge],
				row[beforeEodHeaderIdxBalanced], "", "",
				row[beforeEodHeaderIdxPreviousBalanced], "", "",
				row[beforeEodHeaderIdxFreeTransfer], "",
			})
			outputIDRowMap[rowID] = outputRowLastIndex
			outputRowLastIndex++
		}
	}
	return outputIDRowMap, outputRows, nil
}

// validateHeaders will perform header validation against given columns.
// Will return error when it doesn't match required headers.
func (e *EODProcessor) validateHeaders(headers []string, columns []string) error {
	if len(columns) < len(headers) {
		return ErrInvalidHeader
	}
	for i, header := range headers {
		if columns[i] != header {
			return fmt.Errorf(`invalid row provided for header "%s" at index "%d"`, header, i)
		}
	}
	return nil
}
