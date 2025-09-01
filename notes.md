# Notes

## Assumptions

- The dataset is assumed to be UK domestic, as it only contains `GGM` and `GBP`.
- `GBP` was chosen as the base currency for ranking spenders, marketing people would like that more vs GGM.
- Only `CARD SPEND` transactions are considered for the top5 calculation.
- The `Rate` is consistently treated as the Gold gram price where GGM is on either side, regardless of the transaction direction.

## Choices

- CSV decoding to types is implemented manually to avoid external dependencies. I was playing with the idea of adding `gocsv`.
- `float64` is used for monetary values for simplicity. A dedicated decimal or currency type would be ideal.
- Transactions are streamed from the input via a channel to avoid loading the entire CSV file into memory.
- The tx stream uses a single channel that returns both a transaction and a potential error in a struct. Simpler, compared to managing two separate channels.

## Architecture

- The task was interpreted as a command-line utility, but the core logic operates on `io.Reader` and `io.Writer`. This makes it flexible enough to be used as a CLI tool or integrated into a larger service.

## Ignored edge cases (that I know of)

- Incorrect input: date, missing email, different input order, missing header row.