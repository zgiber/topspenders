package parse

import (
	"bytes"
	"encoding/csv"
	"strconv"
	"testing"
	"time"
)

func TestTopSpenders(t *testing.T) {
	t.Run("happy path with various transactions", func(t *testing.T) {
		t.Parallel()
		// Specific set of transactions to test the core logic.
		transactions := []*Transaction{
			// January
			{FirstName: "A", LastName: "A", Email: "a@test.com", TransactionType: txCardSpend, Amount: 100, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)},
			{FirstName: "B", LastName: "B", Email: "b@test.com", TransactionType: txCardSpend, Amount: 200, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 11, 12, 0, 0, 0, time.UTC)},
			{FirstName: "C", LastName: "C", Email: "c@test.com", TransactionType: txCardSpend, Amount: 50, FromCurrency: currencyGGM, ToCurrency: currencyGBP, Rate: 50, Date: time.Date(2024, 1, 12, 12, 0, 0, 0, time.UTC)}, // 50*50 = 2500 GBP
			{FirstName: "F", LastName: "F", Email: "f@test.com", TransactionType: txCardSpend, Amount: 1000, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)},
			{FirstName: "A", LastName: "A", Email: "a@test.com", TransactionType: txBuyGold, Amount: 999, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)}, // Should be ignored

			// February
			{FirstName: "D", LastName: "D", Email: "d@test.com", TransactionType: txCardSpend, Amount: 300, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 2, 5, 12, 0, 0, 0, time.UTC)},
			{FirstName: "A", LastName: "A", Email: "a@test.com", TransactionType: txCardSpend, Amount: 50, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 2, 6, 12, 0, 0, 0, time.UTC)},
			{FirstName: "E", LastName: "E", Email: "e@test.com", TransactionType: txSellGold, Amount: 10, FromCurrency: currencyGGM, ToCurrency: currencyGBP, Rate: 50, Date: time.Date(2024, 2, 7, 12, 0, 0, 0, time.UTC)}, // Should be ignored
		}

		// Expected output is sorted by month, then by rank (descending spend).
		expectedCSV := `date,rank,amount,currency,transactions,email,firstName,lastName
2024/01,1,2500.0000000,GBP,1,c@test.com,C,C
2024/01,2,1000.0000000,GBP,1,f@test.com,F,F
2024/01,3,200.0000000,GBP,1,b@test.com,B,B
2024/01,4,100.0000000,GBP,1,a@test.com,A,A
2024/02,1,300.0000000,GBP,1,d@test.com,D,D
2024/02,2,50.0000000,GBP,1,a@test.com,A,A
`

		output, err := runTest(t, transactions, Config{StopOnError: false})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if output != expectedCSV {
			t.Errorf("output csv does not match expected value.\nGot:\n%s\nExpected:\n%s", output, expectedCSV)
		}
	})

	t.Run("handles months with fewer than 5 spenders", func(t *testing.T) {
		t.Parallel()
		transactions := []*Transaction{
			{FirstName: "A", LastName: "A", Email: "a@test.com", TransactionType: txCardSpend, Amount: 100, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)},
			{FirstName: "B", LastName: "B", Email: "b@test.com", TransactionType: txCardSpend, Amount: 300, FromCurrency: currencyGBP, ToCurrency: currencyGBP, Rate: 1, Date: time.Date(2024, 1, 11, 12, 0, 0, 0, time.UTC)},
		}

		expectedCSV := `date,rank,amount,currency,transactions,email,firstName,lastName
2024/01,1,300.0000000,GBP,1,b@test.com,B,B
2024/01,2,100.0000000,GBP,1,a@test.com,A,A
`
		output, err := runTest(t, transactions, Config{StopOnError: false})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if output != expectedCSV {
			t.Errorf("output csv does not match expected value.\nGot:\n%s\nExpected:\n%s", output, expectedCSV)
		}
	})

	t.Run("correctly handles stop on error flag", func(t *testing.T) {
		t.Parallel()
		// CSV input with a malformed row.
		csvInput := `First name,Last name,Email,Description,Merchant code,Amount,From Currency,To Currency,Rate,Date
A,A,a@test.com,CARD SPEND,5013,100,GBP,GBP,1,10/01/2024 12:00
B,B,b@test.com,CARD SPEND,5013,invalid_amount,GBP,GBP,1,11/01/2024 12:00
C,C,c@test.com,CARD SPEND,5013,200,GBP,GBP,1,12/01/2024 12:00
`
		inBuffer := bytes.NewBufferString(csvInput)
		outBuffer := &bytes.Buffer{}

		cfg := Config{StopOnError: true}
		err := TopSpenders(inBuffer, outBuffer, cfg)

		if err == nil {
			t.Fatal("expected an error but got nil")
		}

		// Processing should stop on the first error, the output-writing function should never be called.
		if outBuffer.Len() > 0 {
			t.Errorf("expected empty output, but got: %s", outBuffer.String())
		}
	})

	t.Run("continues on error when flag is not set", func(t *testing.T) {
		t.Parallel()
		// Use the same malformed input as the previous test.
		csvInput := `First name,Last name,Email,Description,Merchant code,Amount,From Currency,To Currency,Rate,Date
A,A,a@test.com,CARD SPEND,5013,100,GBP,GBP,1,10/01/2024 12:00
B,B,b@test.com,CARD SPEND,5013,invalid_amount,GBP,GBP,1,11/01/2024 12:00
C,C,c@test.com,CARD SPEND,5013,200,GBP,GBP,1,12/01/2024 12:00
`
		inBuffer := bytes.NewBufferString(csvInput)
		outBuffer := &bytes.Buffer{}

		cfg := Config{StopOnError: false}
		err := TopSpenders(inBuffer, outBuffer, cfg)

		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		// The program should skip the invalid row and produce output for the valid ones.
		expectedCSV := `date,rank,amount,currency,transactions,email,firstName,lastName
2024/01,1,200.0000000,GBP,1,c@test.com,C,C
2024/01,2,100.0000000,GBP,1,a@test.com,A,A
`
		if outBuffer.String() != expectedCSV {
			t.Errorf("output csv does not match expected value.\nGot:\n%s\nExpected:\n%s", outBuffer.String(), expectedCSV)
		}
	})
}

func TestTransaction_validate(t *testing.T) {
	t.Parallel()
	baseTx := func() *Transaction {
		return &Transaction{
			TransactionType: txCardSpend,
			FromCurrency:    currencyGBP,
			ToCurrency:      currencyGGM,
		}
	}

	testCases := []struct {
		name    string
		modFunc func(*Transaction)
		wantErr bool
	}{
		{
			name:    "valid case",
			modFunc: func(tx *Transaction) {},
			wantErr: false,
		},
		{
			name: "invalid transaction type",
			modFunc: func(tx *Transaction) {
				tx.TransactionType = "INVALID_TYPE"
			},
			wantErr: true,
		},
		{
			name: "invalid from currency",
			modFunc: func(tx *Transaction) {
				tx.FromCurrency = "USD"
			},
			wantErr: true,
		},
		{
			name: "invalid to currency",
			modFunc: func(tx *Transaction) {
				tx.ToCurrency = "EUR"
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx := baseTx()
			tc.modFunc(tx)

			err := tx.validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Transaction.validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// runTest is a helper to execute the TopSpenders function with generated data and capture the output.
func runTest(t *testing.T, transactions []*Transaction, cfg Config) (string, error) {
	t.Helper()

	// Convert transactions to CSV in an in-memory buffer
	inBuffer := &bytes.Buffer{}
	csvWriter := csv.NewWriter(inBuffer)

	// Write header
	header := []string{"First name", "Last name", "Email", "Description", "Merchant code", "Amount", "From Currency", "To Currency", "Rate", "Date"}
	if err := csvWriter.Write(header); err != nil {
		t.Fatalf("failed to write csv header: %v", err)
	}

	// Write transaction data
	for _, tx := range transactions {
		record := []string{
			tx.FirstName,
			tx.LastName,
			tx.Email,
			tx.TransactionType,
			tx.MerchantCode,
			strconv.FormatFloat(tx.Amount, 'f', 7, 64),
			tx.FromCurrency,
			tx.ToCurrency,
			strconv.FormatFloat(tx.Rate, 'f', 7, 64),
			tx.Date.Format(timeLayout),
		}
		if err := csvWriter.Write(record); err != nil {
			t.Fatalf("failed to write csv record: %v", err)
		}
	}
	csvWriter.Flush()

	outBuffer := &bytes.Buffer{}
	err := TopSpenders(inBuffer, outBuffer, cfg)

	return outBuffer.String(), err
}
