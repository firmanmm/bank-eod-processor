package bankeodprocessor

import (
	"reflect"
	"testing"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

func TestParser_Execute(t *testing.T) {
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
				},
			},
			&pipeline.EODRowData{
				Index: 1,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
				},
				FreeTransfer:     5,
				AverageBalanced:  4,
				PreviousBalanced: 3,
				Balanced:         2,
			},
			false,
		},
		{
			"Given bad balance then it must fail",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "BAD", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
				},
			},
			nil,
			true,
		},
		{
			"Given bad free transfer then it must fail",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "BAD",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
				},
			},
			nil,
			true,
		},
		{
			"Given bad average balance then it must fail",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "BAD", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
				},
			},
			nil,
			true,
		},
		{
			"Given bad previous balance then it must fail",
			args{
				workerID: 1,
				data: &pipeline.EODRowData{
					Index: 1,
					InputRow: []string{
						"1", "Test 1", "24", "2", "BAD", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
				},
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := make(chan *pipeline.EODRowData, 1)
			parser := NewParser(res)
			if tt.wantErr {
				tt.args.data.FinishChannel = res
			}
			parser.Execute(tt.args.workerID, tt.args.data)
			got := <-res
			if tt.wantErr {
				if !reflect.DeepEqual(tt.wantErr, got.Error != nil) {
					t.Errorf("Parser.Execute() err check = %v, want %v", got.Error != nil, tt.wantErr)
				}
				return
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("Parser.Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
