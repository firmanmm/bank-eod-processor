package bankeodprocessor

import (
	"bytes"
	"context"
	"encoding/csv"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/firmanmm/bank-eod-processor/pipeline"
)

func TestEODProcessor_ProcessSlice(t *testing.T) {

	exactMatchIdx := []int{
		int(afterEodHeaderIdxID),
		int(afterEodHeaderIdxNama),
		int(afterEodHeaderIdxAge),
		int(afterEodHeaderIdxBalanced),
		int(afterEodHeaderIdxAverageBalanced),
		int(afterEodHeaderIdxFreeTransfer),
	}
	closeIndex := []int{
		int(afterEodHeaderIdxNo1Thread),
		int(afterEodHeaderIdxNo2AThread),
		int(afterEodHeaderIdxNo2BThread),
		int(afterEodHeaderIdxNo3Thread),
	}
	type args struct {
		inputRows  [][]string
		outputRows [][]string
	}
	tests := []struct {
		name    string
		args    args
		want    [][]string
		wantErr bool
	}{
		{
			"Given no error then it must succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"3", "Test 3", "25", "110", "0", "1", "150", "125", "1", "5", "1"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
			},
			false,
		},
		{
			"Given no error and mor output then it must succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"44", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"54", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"44", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
				{"54", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				{"3", "Test 3", "25", "110", "0", "1", "150", "125", "1", "5", "1"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
			},
			false,
		},
		{
			"Given no error and partial missing output then it must succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"3", "Test 3", "25", "110", "0", "1", "150", "125", "1", "5", "1"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
			},
			false,
		},
		{
			"Given no error and partial missing first output then it must succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"3", "Test 3", "25", "110", "0", "1", "150", "125", "1", "5", "1"},
			},
			false,
		},
		{
			"Given no error and no output then it must succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"3", "Test 3", "25", "110", "0", "1", "150", "125", "1", "5", "1"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
			},
			false,
		},
		{
			"Given bad column error then it must still succeed",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "BAD", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			[][]string{
				{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
				{"1", "Test 1", "24", "186", "1", "1", "100", "125", "1", "3", "0"},
				{"2", "Test 2", "25", "160", "0", "1", "150", "150", "1", "5", "1"},
				{"3", "Test 3", "25", "176", "", "1", "150", "125", "1", "3", "1"},
				{"4", "Test 4", "25", "110", "0", "1", "100", "100", "1", "5", "1"},
				{"5", "Test 5", "26", "109", "0", "1", "200", "149", "1", "2", "0"},
			},
			false,
		},
		{
			"Given no error and malformed input then it must fail",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			nil,
			true,
		},
		{
			"Given no error and malformed output then it must fail",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			nil,
			true,
		},
		{
			"Given no error and wrong input header then it must fail",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "FreeEEEEE Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			nil,
			true,
		},
		{
			"Given no error and wrong output header then it must fail",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "FreeEEEEE Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			nil,
			true,
		},
		{
			"Given no error and no input then it must fail",
			args{
				inputRows: [][]string{},
				outputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "FreeEEEEE Transfer", "No 2a Thread-No"},
					{"1", "Test 1", "24", "176", "", "", "100", "125", "", "3", ""},
					{"2", "Test 2", "25", "176", "", "", "100", "125", "", "3", ""},
					{"3", "Test 3", "25", "176", "", "", "100", "125", "", "3", ""},
					{"4", "Test 4", "25", "176", "", "", "100", "125", "", "3", ""},
					{"5", "Test 5", "26", "176", "", "", "100", "125", "", "3", ""},
				},
			},
			nil,
			true,
		},
		{
			"Given no error and no output then it must fail",
			args{
				inputRows: [][]string{
					{"id", "Nama", "Age", "Balanced", "Previous Balanced", "Average Balanced", "Free Transfer"},
					{"1", "Test 1", "24", "151", "100", "100", "3"},
					{"2", "Test 2", "25", "150", "150", "100", "2"},
					{"3", "Test 3", "25", "100", "150", "100", "2"},
					{"4", "Test 4", "25", "100", "100", "100", "2"},
					{"5", "Test 5", "26", "99", "200", "120", "2"},
				},
				outputRows: [][]string{},
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bonusDistributor := pipeline.NewBonusDistributor(nil)
			benefitCalculator := pipeline.NewBenefitCalculator(bonusDistributor.Channel())
			averageCalculator := pipeline.NewAverageCalculator(benefitCalculator.Channel())
			parser := NewParser(averageCalculator.Channel())
			eodCalculator := NewEODProcessor(parser)
			got, err := eodCalculator.ProcessSlice(context.Background(), tt.args.inputRows, tt.args.outputRows)
			if (err != nil) != tt.wantErr {
				t.Errorf("EODProcessor.ProcessSlice() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for idx, wantRow := range tt.want {
				gotRow := got[idx]
				if idx == 0 {
					if !reflect.DeepEqual(gotRow, wantRow) {
						t.Errorf("EODProcessor.ProcessSlice() = %v, want %v", gotRow, wantRow)
					}
				} else {
					for _, val := range exactMatchIdx {
						if !reflect.DeepEqual(gotRow[val], wantRow[val]) {
							t.Errorf("EODProcessor.ProcessSlice() exact match index = %v, want %v, idx %v, valIdx %v", gotRow[val], wantRow[val], idx, val)
						}
					}

					for _, val := range closeIndex {
						gotCol := gotRow[val]
						wantCol := wantRow[val]
						if wantCol != "0" {
							if gotCol == "0" {
								t.Errorf("EODProcessor.ProcessSlice() close match index = %v, want %v, idx %v, valIdx %v", gotCol, wantCol, idx, val)
							}
						} else {
							if gotCol != "0" {
								t.Errorf("EODProcessor.ProcessSlice() close match index = %v, want %v, idx %v, valIdx %v", gotCol, wantCol, idx, val)
							}
						}
					}
				}
			}
		})
	}
}

func TestEODProcessor_Process(t *testing.T) {
	bonusDistributor := pipeline.NewBonusDistributor(nil)
	benefitCalculator := pipeline.NewBenefitCalculator(bonusDistributor.Channel())
	averageCalculator := pipeline.NewAverageCalculator(benefitCalculator.Channel())
	parser := NewParser(averageCalculator.Channel())
	eodCalculator := NewEODProcessor(parser)

	inputHandle, _ := os.CreateTemp("", "testdata-eod-input-temp-*")
	inputHandle.WriteString(`id;Nama;Age;Balanced;Previous Balanced;Average Balanced;Free Transfer
1;Test 1;24;151;100;100;3
2;Test 2;25;150;150;100;2
3;Test 3;25;100;150;100;2
4;Test 4;25;100;100;100;2
5;Test 5;26;99;200;120;2`)
	inputPath := inputHandle.Name()
	inputHandle.Close()

	outputHandle, _ := os.CreateTemp("", "testdata-eod-output-temp-*")
	outputHandle.WriteString(`id;Nama;Age;Balanced;No 2b Thread-No;No 3 Thread-No;Previous Balanced;Average Balanced;No 1 Thread-No;Free Transfer;No 2a Thread-No
1;Test 1;36;197;;;164;193;;2;
2;Test 2;39;185;;;71;193;;3;
3;Test 3;51;78;;;81;144;;4;
4;Test 4;39;52;;;106;79;;1;
5;Test 5;34;116;;;61;92;;4;`)
	outputPath := outputHandle.Name()
	outputHandle.Close()

	exactMatchIdx := []int{
		int(afterEodHeaderIdxID),
		int(afterEodHeaderIdxNama),
		int(afterEodHeaderIdxAge),
		int(afterEodHeaderIdxBalanced),
		int(afterEodHeaderIdxAverageBalanced),
		int(afterEodHeaderIdxFreeTransfer),
	}
	closeIndex := []int{
		int(afterEodHeaderIdxNo1Thread),
		int(afterEodHeaderIdxNo2AThread),
		int(afterEodHeaderIdxNo2BThread),
		int(afterEodHeaderIdxNo3Thread),
	}

	eodCalculator.Process(context.Background(), inputPath, outputPath)
	want := [][]string{
		{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"},
		{"1", "Test 1", "36", "186", "1", "1", "100", "125", "1", "3", "0"},
		{"2", "Test 2", "39", "160", "0", "1", "150", "150", "1", "5", "1"},
		{"3", "Test 3", "51", "110", "0", "1", "150", "125", "1", "5", "1"},
		{"4", "Test 4", "39", "110", "0", "1", "100", "100", "1", "5", "1"},
		{"5", "Test 5", "34", "109", "0", "1", "200", "149", "1", "2", "0"},
	}

	targetPayload, err := ioutil.ReadFile(outputPath)
	if err != nil {
		t.Error(err)
	}
	reader := csv.NewReader(bytes.NewReader(targetPayload))
	reader.Comma = ';'
	got, err := reader.ReadAll()
	if err != nil {
		t.Error(err)
	}
	for idx, wantRow := range want {
		gotRow := got[idx]
		if idx == 0 {
			if !reflect.DeepEqual(gotRow, wantRow) {
				t.Errorf("EODProcessor.Process() = %v, want %v", gotRow, wantRow)
			}
		} else {
			for _, val := range exactMatchIdx {
				if !reflect.DeepEqual(gotRow[val], wantRow[val]) {
					t.Errorf("EODProcessor.Process() exact match index = %v, want %v, idx %v, valIdx %v", gotRow[val], wantRow[val], idx, val)
				}
			}

			for _, val := range closeIndex {
				gotCol := gotRow[val]
				wantCol := wantRow[val]
				if wantCol != "0" {
					if gotCol == "0" {
						t.Errorf("EODProcessor.Process() close match index = %v, want %v, idx %v, valIdx %v", gotCol, wantCol, idx, val)
					}
				} else {
					if gotCol != "0" {
						t.Errorf("EODProcessor.Process() close match index = %v, want %v, idx %v, valIdx %v", gotCol, wantCol, idx, val)
					}
				}
			}
		}
	}
}
