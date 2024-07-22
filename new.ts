import csvtojson from "csvtojson";
import { Connection, type ParsedTransactionWithMeta } from "@solana/web3.js";
import bs58 from "bs58";
import { parseMeteoraTransactions } from "./ParseMeteoraTransactions";
import { getDlmmPairs } from "./MeteoraDlmmApi";
import { getJupiterTokenList } from "./JupiterTokenList";
import { MeteoraPositionStream } from "./MeteoraPositionStream";
import { UsdMeteoraPositionStream } from "./UsdMeteoraPositionStream";
import { objArrayToCsvString } from "./util";
import type { MeteoraPosition } from "./MeteoraPosition";
import { getParsedTransactions } from "./ConnectionThrottle";

const errors: string[] = [];
const SIGNATURE_COLUMN_LABEL = process.env.SIGNATURE_COLUMN_LABEL;
if (!SIGNATURE_COLUMN_LABEL) {
  errors.push("SIGNATURE_COLUMN_LABEL environment variable not found");
}
const RPC_URL = process.env.RPC_URL;
if (!RPC_URL) {
  errors.push("RPC_URL environment variable not found");
}
const DATA_FILE = process.env.DATA_FILE;
if (!DATA_FILE) {
  errors.push("DATA_FILE environment variable not found");
}
const START_DATE_STR = process.env.START_DATE;
if (!START_DATE_STR) {
  errors.push("START_DATE environment variable not found");
}
if (errors.length > 0) {
  console.error(errors.join("\n"));
  throw new Error(errors.join("\n"));
}
const START_DATE = new Date(START_DATE_STR!);

interface LpArmyStudentData {
  fullSubmission: { [key: string]: string };
  originalSignature: string;
  cleansedSignature?: string;
  position?: string;
  meteoraPosition?: MeteoraPosition;
}

// Load from CSV
const output: LpArmyStudentData[] = [];
const file = Bun.file(process.env.DATA_FILE!);
const text = await file.text();
const fullOriginalData = (await csvtojson().fromString(text)) as {
  [key: string]: string;
}[];
const originalData = fullOriginalData.map(
  (data) => data[SIGNATURE_COLUMN_LABEL!],
);

function isValidSignature(signature: string) {
  if (signature.length < 86 || signature.length > 88) {
    return false;
  }
  try {
    bs58.decode(signature);
    return true;
  } catch (err) {
    return false;
  }
}

// Get the cleansed transaction IDs and start building the output
originalData.forEach((original, index) => {
  const cleansedTxId = original?.replace("https://solscan.io/tx/", "");
  if (!cleansedTxId) {
    return output.push({
      fullSubmission: fullOriginalData[index],
      originalSignature: original,
    });
  }
  if (isValidSignature(cleansedTxId)) {
    return output.push({
      fullSubmission: fullOriginalData[index],
      originalSignature: original,
      cleansedSignature: cleansedTxId,
    });
  }
  return output.push({
    fullSubmission: fullOriginalData[index],
    originalSignature: original,
  });
});

const submittedSignatures = output
  .filter((outputData) => outputData.cleansedSignature != null)
  .map((outputData) => outputData.cleansedSignature) as string[];
const connection = new Connection(process.env.RPC_URL!);

// Get parsed transactions so we can get the position addresses
const parsedTransactions = (await getParsedTransactions(
  connection,
  submittedSignatures,
  {
    maxSupportedTransactionVersion: 0,
    commitment: "confirmed",
  },
)) as ParsedTransactionWithMeta[];

console.log(
  `Read ${parsedTransactions.length} initial transactions, getting position addresses...`,
);

const { pairs } = await getDlmmPairs();
const tokenList = await getJupiterTokenList();

const meteoraTransactions = (
  await Promise.all(
    parsedTransactions.map((transaction) =>
      parseMeteoraTransactions(connection, pairs, tokenList, transaction),
    ),
  )
).flat();

meteoraTransactions.forEach((transaction) => {
  const match = output.find(
    (outputData) => outputData.cleansedSignature == transaction.signature,
  );
  if (match) {
    match.position = transaction.position;
  }
});

console.log("Getting all P&Ls...");

output
  .filter((outputData) => outputData.position)
  .forEach(async (outputData) => {
    new MeteoraPositionStream(connection, outputData.position!).on(
      "data",
      (positionStreamData) => {
        if (positionStreamData.type == "positionsAndTransactions") {
          const { positions } = positionStreamData;
          new UsdMeteoraPositionStream(positions).on(
            "data",
            (usdPositionStreamData) => {
              if (usdPositionStreamData.type == "updatedPosition") {
                const updatedPosition = usdPositionStreamData.updatedPosition;
                outputData.meteoraPosition = updatedPosition;
                const completedCount = output.filter(
                  (outputData) => outputData.meteoraPosition !== undefined,
                ).length;
                const csvString = objArrayToCsvString(
                  output.map((data) => {
                    if (data.meteoraPosition) {
                      const profitPercent =
                        -data.meteoraPosition.usdProfitLossValue! /
                        data.meteoraPosition.usdDepositsValue!;
                      const openDateObj = new Date(
                        data.meteoraPosition.openTimestampMs,
                      );
                      const openDate = openDateObj.toISOString();
                      const validProfitPercent = profitPercent > 0.05;
                      const validDate = openDateObj > START_DATE;
                      const validSubmission = validProfitPercent && validDate;
                      return {
                        ...data.fullSubmission,
                        profitPercent,
                        openDate,
                        validProfitPercent,
                        validDate,
                        validSubmission,
                        ...data.meteoraPosition,
                      };
                    }
                    return {
                      ...data.fullSubmission,
                      profitPercent: null,
                      openDate: null,
                      validProfitPercent: null,
                      validDate: null,
                      validSubmission: null,
                      position: null,
                      lbPair: null,
                      sender: null,
                      pairName: null,
                      mintX: null,
                      mintY: null,
                      mintXDecimals: null,
                      mintYDecimals: null,
                      reward1Mint: null,
                      reward2Mint: null,
                      symbolX: null,
                      symbolY: null,
                      symbolReward1: null,
                      symbolReward2: null,
                      isClosed: null,
                      isHawksight: null,
                      transactions: null,
                      transactionCount: null,
                      openTimestampMs: null,
                      closeTimestampMs: null,
                      totalXDeposits: null,
                      totalYDeposits: null,
                      usdTotalXDeposits: null,
                      usdTotalYDeposits: null,
                      totalOpenXBalance: null,
                      totalOpenYBalance: null,
                      usdTotalOpenXBalance: null,
                      usdTotalOpenYBalance: null,
                      depositCount: null,
                      totalXWithdraws: null,
                      totalYWithdraws: null,
                      usdTotalXWithdraws: null,
                      usdTotalYWithdraws: null,
                      withdrawCount: null,
                      netXDepositsAndWithdraws: null,
                      netYDepositsAndWithdraws: null,
                      totalClaimedXFees: null,
                      totalClaimedYFees: null,
                      totalClaimedFeesValue: null,
                      usdClaimedXFees: null,
                      usdClaimedYFees: null,
                      totalUnclaimedXFees: null,
                      totalUnclaimedYFees: null,
                      usdTotalUnclaimedXFees: null,
                      usdTotalUnclaimedYFees: null,
                      totalXFees: null,
                      totalYFees: null,
                      usdTotalXFees: null,
                      usdTotalYFees: null,
                      feeClaimCount: null,
                      totalReward1: null,
                      totalReward2: null,
                      usdTotalReward1: null,
                      usdTotalReward2: null,
                      rewardClaimClount: null,
                      inverted: null,
                      isOneSided: null,
                      hasNoIl: null,
                      hasNoFees: null,
                      depositsValue: null,
                      hasApiError: null,
                      usdDepositsValue: null,
                      withdrawsValue: null,
                      usdWithdrawsValue: null,
                      netDepositsAndWithdrawsValue: null,
                      usdNetDepositsAndWithdrawsValue: null,
                      openBalanceValue: null,
                      claimedFeesValue: null,
                      unclaimedFeesValue: null,
                      totalFeesValue: null,
                      profitLossValue: null,
                      usdOpenBalanceValue: null,
                      usdClaimedFeesValue: null,
                      usdUnclaimedFeesValue: null,
                      usdTotalFeesValue: null,
                      usdProfitLossValue: null,
                    };
                  }),
                );
                Bun.write("./out.csv", csvString);
                console.log(
                  `Obtained P&L for ${completedCount} of ${meteoraTransactions.length} positions`,
                );
              }
            },
          );
        }
      },
    );
  });
