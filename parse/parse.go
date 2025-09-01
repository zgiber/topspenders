package parse

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"time"
)

const (
	timeLayout = "02/01/2006 15:04"

	txCardSpend = "CARD SPEND"
	txBuyGold   = "BUY GOLD"
	txSellGold  = "SELL GOLD"

	currencyGBP = "GBP"
	currencyGGM = "GGM"

	currencyPrecisionDecimals = 7
)

type Transaction struct {
	FirstName       string
	LastName        string
	Email           string
	TransactionType string
	MerchantCode    string
	Amount          float64
	FromCurrency    string
	ToCurrency      string
	Rate            float64
	Date            time.Time
}

func (t *Transaction) validate() error {
	switch t.TransactionType {
	case txBuyGold, txSellGold, txCardSpend:
	default:
		return fmt.Errorf("unknown transaction type: %s", t.TransactionType)
	}

	switch t.FromCurrency {
	case currencyGBP, currencyGGM:
	default:
		return fmt.Errorf("unsupported currency")
	}

	switch t.ToCurrency {
	case currencyGBP, currencyGGM:
	default:
		return fmt.Errorf("unsupported currency")
	}

	return nil
}

type UserMonthlySpending struct {
	FirstName        string
	LastName         string
	Email            string
	TotalGBP         float64
	TransactionCount int
}

func (us *UserMonthlySpending) update(tx *Transaction) {
	// We track spending in GBP: marketing purposes.
	if tx.FromCurrency == currencyGGM {
		us.TotalGBP += tx.Amount * tx.Rate
	}

	if tx.FromCurrency == currencyGBP {
		us.TotalGBP += tx.Amount
	}

	us.TransactionCount++
}

type Config struct {
	StopOnError bool
}

type parsedTx struct {
	tx  *Transaction
	err error
}

// TopSpenders processes a CSV of transactions and writes the top 5 spenders per month.
func TopSpenders(transactionsList io.Reader, results io.Writer, cfg Config) error {
	// Streaming on channels allows us not to fit he entire list in memory.
	transactions := newTxStream(transactionsList)

	// yearmonth:email:spending
	monthlySpendings := map[int]map[string]*UserMonthlySpending{}

	// We write responses sorted by date.
	// May remove if undesired.
	for parsed := range transactions {
		if parsed.err != nil {
			if cfg.StopOnError {
				return parsed.err
			}
			// TODO: find a neater solution to separate the error from the output
			// not everyone separates stdout from stderr
			slog.Error("input error", "error", parsed.err)
			continue
		}

		tx := parsed.tx
		if tx.TransactionType != txCardSpend {
			// We are only interested in 'CARD SPEND' transactions.
			continue
		}
		key := monthKey(tx.Date)
		// Initialise the nested map if it is an unseen month
		month, ok := monthlySpendings[key]
		if !ok {
			month = map[string]*UserMonthlySpending{}
			monthlySpendings[key] = month
		}

		userSpendings, ok := month[tx.Email]
		if !ok {
			userSpendings = &UserMonthlySpending{
				FirstName: tx.FirstName,
				LastName:  tx.LastName,
				Email:     tx.Email,
			}
			month[tx.Email] = userSpendings
		}
		userSpendings.update(tx)
	}

	return writeMonthlySpendings(monthlySpendings, results)
}

func writeMonthlySpendings(spendings map[int]map[string]*UserMonthlySpending, w io.Writer) error {
	monthsSeen := make([]int, 0, len(spendings))
	for m := range spendings {
		monthsSeen = append(monthsSeen, m)
	}
	sort.Ints(monthsSeen)

	csvWriter := csv.NewWriter(w)
	csvWriter.Write([]string{
		"date",
		"rank",
		"amount",
		"currency",
		"transactions",
		"email",
		"firstName",
		"lastName",
	})
	for _, key := range monthsSeen {
		month := spendings[key]
		userSpendings := make([]*UserMonthlySpending, 0, len(month))
		for _, spendings := range month {
			userSpendings = append(userSpendings, spendings)
		}
		sort.Slice(userSpendings, func(i int, j int) bool {
			// sort descending by TotalGBP
			return userSpendings[i].TotalGBP > userSpendings[j].TotalGBP
		})

		topN := 5
		if len(userSpendings) < topN {
			topN = len(userSpendings)
		}
		for i := 0; i < topN; i++ {
			userSpending := userSpendings[i]
			rank := i + 1
			date := time.Date(key/100, time.Month(key%100), 1, 0, 0, 0, 0, time.UTC)
			err := csvWriter.Write([]string{
				date.Format("2006/01"),
				strconv.Itoa(rank),
				strconv.FormatFloat(userSpending.TotalGBP, 'f', currencyPrecisionDecimals, 64),
				"GBP",
				strconv.Itoa(userSpending.TransactionCount),
				userSpending.Email,
				userSpending.FirstName,
				userSpending.LastName,
			})
			if err != nil {
				return err
			}
		}
	}
	csvWriter.Flush()
	return csvWriter.Error()
}

// monthKey creates a sortable integer key from a date, e.g., 2024/07 -> 202407.
func monthKey(date time.Time) int {
	return date.Year()*100 + int(date.Month())
}

func newTxStream(transactionsList io.Reader) chan parsedTx {
	csvReader := csv.NewReader(transactionsList)
	txChan := make(chan parsedTx, 1)

	go func() {

		// skip input headers
		// TODO: check if there are headers at all
		if _, err := csvReader.Read(); err != nil {
			txChan <- parsedTx{err: err}
			close(txChan)
			return
		}

		for {
			record, err := csvReader.Read()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					// If we're not finished with the input yet, return the error.
					txChan <- parsedTx{err: err}
				}
				// io.EOF signals that we reached the end of the input
				close(txChan)
				return
			}

			tx, err := decodeRecord(record)
			if err != nil {
				// Caller may decide whether to stop the whole process
				// when input errors are detected.
				// For now, we continue.
				txChan <- parsedTx{err: err}
				continue
			}

			if err := tx.validate(); err != nil {
				txChan <- parsedTx{err: err}
				continue
			}

			txChan <- parsedTx{tx: tx}
		}
	}()

	return txChan
}

func decodeRecord(record []string) (*Transaction, error) {
	if l := len(record); l < 10 {
		return nil, fmt.Errorf("invalid number of columns: %v < 10", l)
	}

	amount, err := strconv.ParseFloat(record[5], 64)
	if err != nil {
		return nil, err
	}
	rate, err := strconv.ParseFloat(record[8], 64)
	if err != nil {
		return nil, err
	}

	date, err := time.Parse(timeLayout, record[9])
	if err != nil {
		return nil, fmt.Errorf("invalid time format: %s", record[9])
	}

	return &Transaction{
		FirstName:       record[0],
		LastName:        record[1],
		Email:           record[2],
		TransactionType: record[3],
		MerchantCode:    record[4],
		Amount:          amount,
		FromCurrency:    record[6],
		ToCurrency:      record[7],
		Rate:            rate,
		Date:            date,
	}, nil
}
