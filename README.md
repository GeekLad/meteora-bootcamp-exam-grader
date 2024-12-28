# Meteora Bootcamp Exam Grader

This is a script GeekLad created to grade submissions for the LP Army Bootcamp.
It is heavily based on his [Meteora DLMM Profit Analysis Tool](https://github.com/GeekLad/meteora-profit-analysis/).

## Environment Variables

- `RPC_URL`: URL of the RPC to use for parsing transactions
- `ORIGIN_URL`: Origin header for RPC requests
- `DATA_FILE`: The filename of the CSV that contains the submissions
- `SIGNATURE_COLUMN_LABEL`: The label for the column in the file that contains
  the transaction signature submitted
- `WALLET_COLUMN_LABEL`: The label for the column with the wallet address
- `MIN_USD_DEPOSIT_VALUE`: The minimum USD deposit value
- `MIN_HOURS_OPEN`: The minimum # of hours the position needs to be open
- `MIN_PROFIT_PERCENT`: The minimium profit percent
- `START_DATE`: The start date for the exam
- `END_DATE`: The end date for the exam
- `THROTTLE_LIMIT`: # of transactions per each interval for throttling
- `THROTTLE_INTERVAL`: The throttle interval in milliseconds

## To Launch

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

This project was created using `bun init` in bun v1.1.20. [Bun](https://bun.sh) is a fast all-in-one JavaScript runtime.
