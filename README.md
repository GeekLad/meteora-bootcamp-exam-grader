# Meteora Bootcamp Exam Grader

This is a script GeekLad created to grade submissions for the LP Army Bootcamp.
It is heavily based on his [Meteora DLMM Profit Analysis Tool](https://github.com/GeekLad/meteora-profit-analysis/).

## Environment Variables

- `RPC_URL`: URL of the RPC to use for parsing transactions
- `DATA_FILE`: The filename of the CSV that contains the submissions
- `SIGNATURE_COLUMN_NUMBER`: The column in the file that contains the transaction signature submitted

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
