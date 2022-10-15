package bankeodprocessor

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

func TestWriter_Execute(t *testing.T) {
	type args struct {
		workerID int
		data     *pipeline.EODRowData
	}
	tests := []struct {
		name    string
		args    args
		want    *pipeline.EODRowData
		wantErr bool
	}{
		{
			"Given no error then it must succeed",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
					AverageBalanced:  1,
					PreviousBalanced: 2,
					Balanced:         3,
					FreeTransfer:     4,
					ThreadNo1:        11,
					ThreadNo2A:       21,
					ThreadNo2B:       31,
					ThreadNo3:        41,
				},
			},
			&pipeline.EODRowData{
				Index: 1,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "3", "31", "41", "100", "1", "11", "4", "21",
				},
				AverageBalanced:  1,
				PreviousBalanced: 2,
				Balanced:         3,
				FreeTransfer:     4,
				ThreadNo1:        11,
				ThreadNo2A:       21,
				ThreadNo2B:       31,
				ThreadNo3:        41,
			},
			false,
		},
		{
			"Given an error then it must still succeed",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
					AverageBalanced:  1,
					PreviousBalanced: 2,
					Balanced:         3,
					FreeTransfer:     4,
					ThreadNo1:        11,
					ThreadNo2A:       21,
					ThreadNo2B:       31,
					ThreadNo3:        41,
					Error:            errors.New("an error"),
				},
			},
			&pipeline.EODRowData{
				Index: 1,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "176", "", "an error", "100", "125", "", "3", "",
				},
				AverageBalanced:  1,
				PreviousBalanced: 2,
				Balanced:         3,
				FreeTransfer:     4,
				ThreadNo1:        11,
				ThreadNo2A:       21,
				ThreadNo2B:       31,
				ThreadNo3:        41,
				Error:            errors.New("an error"),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &sync.WaitGroup{}
			wg.Add(1)
			writer := NewWriter(wg)
			writer.Execute(tt.args.workerID, tt.args.data)
			got := tt.args.data
			if tt.wantErr {
				if !reflect.DeepEqual(tt.wantErr, got.Error != nil) {
					t.Errorf("Writer.Execute() err check = %v, want %v", got.Error != nil, tt.wantErr)
				}
				return
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Writer.Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
