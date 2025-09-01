package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/zgiber/topspenders/parse"
)

func main() {
	stopOnError := flag.Bool("stop-on-error", false, "Stop processing on the first parsing error")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: topspenders [-stop-on-error] <input.csv>")
		os.Exit(1)
	}
	filePath := flag.Args()[0]

	inputFile, err := os.Open(filePath)
	if err != nil {
		slog.Error("failed to open input file", "path", filePath, "error", err)
		os.Exit(1)
	}
	defer inputFile.Close()

	cfg := parse.Config{
		StopOnError: *stopOnError,
	}
	if err := parse.TopSpenders(inputFile, os.Stdout, cfg); err != nil {
		slog.Error("failed to process transactions", "error", err)
		os.Exit(1)
	}
}
