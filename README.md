# Top Spenders CLI

Aa command-line tool that processes a CSV file of user transactions to identify the top 5 spenders for each month.

## Overview

The tool reads a list of transactions, filters for card spending, aggregates the total amount spent by each user for each month, and outputs a ranked list of the top 5 spenders. 

## Usage

### Prerequisites

-   Go 1.24 or later.

### Build

To build the executable, run the following command from the project root:

```sh
go build -o topspenders ./cmd/main.go
```

### Run

Execute the tool by providing the path to the input CSV file as an argument. The results will be printed to standard output.

```sh
./topspenders ./test/sample-transactions.csv
```

#### Error Handling

By default, the tool will log any parsing errors to `stderr` and continue processing the rest of the file.

To make the tool exit immediately upon encountering the first parsing error, use the `-stop-on-error` flag:

```sh
./topspenders -stop-on-error ./test/sample-transactions.csv
```

## Testing

To run the full suite of tests for the project, use the following command:

```sh
go test ./...
```
