import csvtojson from "csvtojson";
import { Connection, type ParsedTransactionWithMeta } from "@solana/web3.js";
import bs58 from "bs58";
import { parseMeteoraTransactions } from "./ParseMeteoraTransactions";
import { getDlmmPairs } from "./MeteoraDlmmApi";
import { getJupiterTokenList } from "./JupiterTokenList";
import { MeteoraPositionStream } from "./MeteoraPositionStream";
import { UsdMeteoraPositionStream } from "./UsdMeteoraPositionStream";
import { delay, objArrayToCsvString } from "./util";
import type { MeteoraPosition } from "./MeteoraPosition";
import { getParsedTransactions } from "./ConnectionThrottle";

const errors: string[] = [];
const requiredEnvVariables = [
  "SIGNATURE_COLUMN_LABEL",
  "WALLET_COLUMN_LABEL",
  "RPC_URL",
  "DATA_FILE",
  "START_DATE",
  "MIN_USD_DEPOSIT_VALUE",
  "MIN_HOURS_OPEN",
  "START_DATE",
  "END_DATE",
];

function environmentCheck(variable: string) {
  if (!process.env[variable]) {
    errors.push(`${variable} environment variable not found`);
  }
}

requiredEnvVariables.forEach((variable) => environmentCheck(variable));

if (errors.length > 0) {
  console.error(errors.join("\n"));
  throw new Error(errors.join("\n"));
}

const SIGNATURE_COLUMN_LABEL = process.env.SIGNATURE_COLUMN_LABEL!;
const WALLET_COLUMN_LABEL = process.env.WALLET_COLUMN_LABEL!;
const RPC_URL = process.env.RPC_URL!;
const DATA_FILE = process.env.DATA_FILE!;
const MIN_USD_DEPOSIT_VALUE = Number(process.env.MIN_USD_DEPOSIT_VALUE!);
const MIN_PROFIT_PERCENT = process.env.MIN_PROFIT_PERCENT
  ? Number(process.env.MIN_PROFIT_PERCENT)
  : 0;
const MIN_HOURS_OPEN = Number(process.env.MIN_HOURS_OPEN!);
const START_DATE = new Date(process.env.START_DATE!);
const END_DATE = new Date(process.env.END_DATE!);
const THROTTLE_LIMIT = Number(process.env.THROTTLE_LIMIT || 10);

interface LpArmyStudentData {
  fullSubmission: { [key: string]: string };
  originalSignature: string;
  originalWallet: string;
  cleansedSignature?: string;
  cleansedWallet?: string;
  position?: string;
  meteoraPosition?: MeteoraPosition;
}

// Load from CSV
const output: LpArmyStudentData[] = [];
const file = Bun.file(DATA_FILE);
const text = await file.text();
const fullOriginalData = (await csvtojson().fromString(text)) as {
  [key: string]: string;
}[];

// Make sure the signature column label exists
if (!fullOriginalData[0][SIGNATURE_COLUMN_LABEL]) {
  throw new Error(
    `Column labeled "${SIGNATURE_COLUMN_LABEL!}" was not found in input data`,
  );
}

const originalData = fullOriginalData.map(
  (data) => data[SIGNATURE_COLUMN_LABEL],
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

function isValidWallet(walletAddress: string) {
  if (walletAddress.length < 43 || walletAddress.length > 44) {
    return false;
  }
  try {
    bs58.decode(walletAddress);
    return true;
  } catch (err) {
    return false;
  }
}

function chunkArray<T>(array: T[], size: number): T[][] {
  if (size <= 0) throw new Error("Size must be greater than 0");
  const result: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

// Get the cleansed transaction IDs and start building the output
originalData.forEach((originalSignature, index) => {
  const cleansedSignature = originalSignature?.replace(
    /https:\/\/([^\/]+\/)+/,
    "",
  );
  const originalWallet = fullOriginalData[index][WALLET_COLUMN_LABEL];
  const cleansedWallet = originalWallet?.replace(/https:\/\/([^\/]+\/)+/, "");
  if (!cleansedSignature) {
    return output.push({
      fullSubmission: fullOriginalData[index],
      originalSignature,
      originalWallet,
    });
  }
  if (isValidSignature(cleansedSignature) && isValidWallet(cleansedWallet)) {
    return output.push({
      fullSubmission: fullOriginalData[index],
      originalSignature,
      originalWallet,
      cleansedSignature,
      cleansedWallet,
    });
  }
  return output.push({
    fullSubmission: fullOriginalData[index],
    originalSignature: originalSignature,
    originalWallet: originalWallet,
  });
});

const submittedSignatures = output
  .filter((outputData) => outputData.cleansedSignature != null)
  .map((outputData) => outputData.cleansedSignature) as string[];
const connection = new Connection(
  RPC_URL,
  process.env.ORIGIN_URL
    ? {
        httpHeaders: {
          Origin: process.env.ORIGIN_URL,
        },
      }
    : undefined,
);

// Chunk into groups
const chunkedSignatures = chunkArray(submittedSignatures, 250);

// Array to store all the results
const parsedTransactions: ParsedTransactionWithMeta[] = [];

// Process the chunks and store the results into parsedTransactions
let processedCount = 0;
for (let x = 0; x < chunkedSignatures.length; x++) {
  let currentSignatures = chunkedSignatures[x];
  // Get parsed transactions so we can get the position addresses
  const currentTransactions = (await getParsedTransactions(
    connection,
    currentSignatures,
    {
      maxSupportedTransactionVersion: 0,
      commitment: "confirmed",
    },
  )) as ParsedTransactionWithMeta[];

  // Add the results
  parsedTransactions.push(...currentTransactions);

  processedCount += currentSignatures.length;
  console.log(
    `Read ${processedCount} of ${submittedSignatures.length} initial transactions.`,
  );
}

console.log(
  `Read ${parsedTransactions.length} initial transactions, getting position addresses...`,
);

const { pairs } = await getDlmmPairs();
const tokenList = await getJupiterTokenList();

let positionsFound = 0;
let concurrentProcessCount = 0;
const meteoraTransactions = (
  await Promise.all(
    parsedTransactions.map(async (transaction) => {
      while (concurrentProcessCount >= THROTTLE_LIMIT) {
        await delay(100);
      }
      concurrentProcessCount++;
      const result = await parseMeteoraTransactions(
        connection,
        pairs,
        tokenList,
        transaction,
      );
      concurrentProcessCount--;
      positionsFound++;
      console.log(
        `Obtained ${positionsFound} out of ${parsedTransactions.length} position addresses`,
      );
      return result;
    }),
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
    while (concurrentProcessCount >= THROTTLE_LIMIT) {
      await delay(100);
    }
    concurrentProcessCount++;
    const completedCount = output.filter(
      (outputData) => outputData.meteoraPosition !== undefined,
    ).length;
    console.log(
      `Updating row ${output.indexOf(outputData) + 2}, Wallet address: ${
        outputData.originalWallet
      }.  Obtained P&L for ${completedCount} of ${
        meteoraTransactions.length
      } positions.`,
    );
    new MeteoraPositionStream(connection, outputData.position!)
      .on("data", (positionStreamData) => {
        if (positionStreamData.type == "positionsAndTransactions") {
          const { positions } = positionStreamData;
          new UsdMeteoraPositionStream(positions).on(
            "data",
            (usdPositionStreamData) => {
              if (usdPositionStreamData.type == "updatedPosition") {
                const updatedPosition = usdPositionStreamData.updatedPosition;
                outputData.meteoraPosition = updatedPosition;
                const csvString = objArrayToCsvString(
                  output.map((data) => {
                    if (data.meteoraPosition) {
                      const usdProfitPercent =
                        data.meteoraPosition.usdDepositsValue !== null &&
                        data.meteoraPosition.usdProfitLossValue !== null
                          ? -data.meteoraPosition.usdProfitLossValue! /
                            data.meteoraPosition.usdDepositsValue!
                          : null;
                      const quoteProfitPercent =
                        -data.meteoraPosition.profitLossValue /
                        data.meteoraPosition.depositsValue;
                      const openDateObj = new Date(
                        data.meteoraPosition.openTimestampMs,
                      );
                      const closeDateObj = new Date(
                        data.meteoraPosition.closeTimestampMs,
                      );
                      const openDate = openDateObj.toISOString();
                      const closeDate = closeDateObj.toISOString();
                      const validProfitPercent =
                        usdProfitPercent !== null && usdProfitPercent < 10
                          ? usdProfitPercent > MIN_PROFIT_PERCENT / 100
                          : quoteProfitPercent > MIN_PROFIT_PERCENT;
                      const validDate =
                        openDateObj >= START_DATE && closeDateObj <= END_DATE;
                      const validTimeOpen =
                        END_DATE.getTime() - START_DATE.getTime() >
                        MIN_HOURS_OPEN * 1000 * 60 * 60;
                      const validUsdAmount =
                        data.meteoraPosition.usdDepositsValue !== null &&
                        -data.meteoraPosition.usdDepositsValue >
                          MIN_USD_DEPOSIT_VALUE;
                      const hasApiError = updatedPosition.hasApiError;
                      const validWallet =
                        data.cleansedWallet == data.meteoraPosition.sender;
                      const isClosed = data.meteoraPosition.isClosed;
                      const validSubmission =
                        validProfitPercent &&
                        validUsdAmount &&
                        validDate &&
                        validTimeOpen &&
                        validWallet &&
                        isClosed &&
                        !hasApiError;
                      return {
                        ...data.fullSubmission,
                        usdDepositAmount: data.meteoraPosition.usdDepositsValue
                          ? -data.meteoraPosition.usdDepositsValue
                          : null,
                        usdProfitPercent,
                        quoteProfitPercent,
                        openDate,
                        closeDate,
                        validProfitPercent,
                        validDate,
                        validTimeOpen,
                        validUsdAmount,
                        validWallet,
                        validSubmission,
                        ...data.meteoraPosition,
                      };
                    }
                    return {
                      ...data.fullSubmission,
                      usdDepositAmount: null,
                      usdProfitPercent: null,
                      quoteProfitPercent: null,
                      openDate: null,
                      closeDate: null,
                      validProfitPercent: null,
                      validDate: null,
                      validTimeOpen: null,
                      validUsdAmount: null,
                      validWallet: null,
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
              }
            },
          );
        }
      })
      .on("end", () => {
        concurrentProcessCount--;
      });
  });
