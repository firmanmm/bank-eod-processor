package pipeline

import (
	"reflect"
	"testing"
)

func TestBonusDistributor_Execute(t *testing.T) {
	type args struct {
		workerID       int
		isFinalChannel bool

		data *EODRowData
	}
	tests := []struct {
		name    string
		args    args
		want    *EODRowData
		wantErr bool
	}{
		{
			"Given no error then it must succeed",
			args{
				workerID:       1,
				isFinalChannel: false,
				data: &EODRowData{
					Index: 99,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
					AverageBalanced:  100,
					PreviousBalanced: 100,
					Balanced:         200,
					FreeTransfer:     4,
					ThreadNo1:        111,
					ThreadNo2A:       21,
					ThreadNo2B:       31,
				},
			},
			&EODRowData{
				Index: 99,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
				},
				AverageBalanced:  100,
				PreviousBalanced: 100,
				Balanced:         210,
				FreeTransfer:     4,
				ThreadNo1:        111,
				ThreadNo2A:       21,
				ThreadNo2B:       31,
				ThreadNo3:        1,
			},
			false,
		},
		{
			"Given no error and outside range then it must succeed",
			args{
				workerID:       1,
				isFinalChannel: false,
				data: &EODRowData{
					Index: 100,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
					AverageBalanced:  100,
					PreviousBalanced: 100,
					Balanced:         200,
					FreeTransfer:     4,
					ThreadNo1:        111,
					ThreadNo2A:       21,
					ThreadNo2B:       31,
				},
			},
			&EODRowData{
				Index: 100,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
				},
				AverageBalanced:  100,
				PreviousBalanced: 100,
				Balanced:         200,
				FreeTransfer:     4,
				ThreadNo1:        111,
				ThreadNo2A:       21,
				ThreadNo2B:       31,
			},
			false,
		},
		{
			"Given no error and final channel then it must succeed",
			args{
				workerID:       1,
				isFinalChannel: true,
				data: &EODRowData{
					Index: 99,
					InputRow: []string{
						"1", "Test 1", "24", "2", "3", "4", "5",
					},
					OutputRow: []string{
						"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
					},
					AverageBalanced:  100,
					PreviousBalanced: 100,
					Balanced:         200,
					FreeTransfer:     4,
					ThreadNo1:        111,
					ThreadNo2A:       21,
					ThreadNo2B:       31,
				},
			},
			&EODRowData{
				Index: 99,
				InputRow: []string{
					"1", "Test 1", "24", "2", "3", "4", "5",
				},
				OutputRow: []string{
					"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", "",
				},
				AverageBalanced:  100,
				PreviousBalanced: 100,
				Balanced:         210,
				FreeTransfer:     4,
				ThreadNo1:        111,
				ThreadNo2A:       21,
				ThreadNo2B:       31,
				ThreadNo3:        1,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := make(chan *EODRowData, 1)
			constructorRes := res
			if tt.args.isFinalChannel {
				constructorRes = nil
				tt.args.data.FinishChannel = res
			}
			distributor := NewBonusDistributor(constructorRes)
			if tt.wantErr {
				tt.args.data.FinishChannel = res
			}
			distributor.Execute(tt.args.workerID, tt.args.data)
			got := <-res
			tt.args.data.FinishChannel = nil
			if tt.wantErr {
				if !reflect.DeepEqual(tt.wantErr, got.Error != nil) {
					t.Errorf("BonusDistributor.Execute() err check = %v, want %v", got.Error != nil, tt.wantErr)
				}
				return
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("BonusDistributor.Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
