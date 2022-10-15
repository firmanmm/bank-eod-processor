package main

import (
	"context"
	"flag"
	"log"

	bankeodprocessor "github.com/firmanmm/bank-eod-processor"
	"github.com/firmanmm/bank-eod-processor/pipeline"
)

const (
	defaultInputFile  = "Before Eod.csv"
	defaultOutputFile = "After Eod.csv"
)

func main() {
	inputFlag := flag.String("input", defaultInputFile, "File name to be used as input (required)")
	outputFlag := flag.String("output", defaultOutputFile, "File name to be used as an output (optional)")
	flag.Parse()
	input := *inputFlag
	output := *outputFlag
	if len(input) == 0 {
		log.Fatalln("Input can't be empty")
	}
	// output is optional and will default output name if not provided.
	if len(output) == 0 {
		output = defaultOutputFile
	}
	bonusDistributor := pipeline.NewBonusDistributor(nil)
	benefitCalculator := pipeline.NewBenefitCalculator(bonusDistributor.Channel())
	averageCalculator := pipeline.NewAverageCalculator(benefitCalculator.Channel())
	parser := bankeodprocessor.NewParser(averageCalculator.Channel())
	eodCalculator := bankeodprocessor.NewEODProcessor(parser)
	if err := eodCalculator.Process(context.Background(), input, output); err != nil {
		log.Fatalln(err)
	}
}
