// gs_geocode_csv.js
//
// Author: Gustavo Adrián Salvini
// Personal website: https://gustavosalvini.com.ar
// GitHub: https://github.com/guspatagonico/
// X: @guspatagonico
// Mastodon: https://techhub.social/@guspatagonico

// Required modules
require("dotenv").config(); // Load environment variables from .env file
const config = require("config"); // For loading configuration files
const fs = require("fs");
const path = require("path"); // For manipulating file paths
const fsp = fs.promises;
const csvParser = require("csv-parser");
const fastcsv = require("fast-csv");
const axios = require("axios");

// --- Configuration (Defaults, can be overridden by config/default.json) ---
const DEFAULT_INPUT_CSV_PATH = "input.csv";
const DEFAULT_BASE_OUTPUT_CSV_PATH = "output.csv";
const DEFAULT_ADDRESS_COLUMN_INDEX = 9; // 10th column (0-indexed)
const DEFAULT_REQUEST_DELAY_MS = 200;
const DEFAULT_MAX_RECORDS_TO_PROCESS = 0; // 0 or negative means no limit for this run.
const DEFAULT_BATCH_WRITE_SIZE = 10;
const DEFAULT_FROM_ROW = 0; // Default starting row for processing
const DEFAULT_LOGS_FOLDER = "logs"; // Default folder for logs

// Load from config file (flattened structure) or use defaults
const INPUT_CSV_PATH = config.has("inputCsvPath")
  ? config.get("inputCsvPath")
  : DEFAULT_INPUT_CSV_PATH;
const BASE_OUTPUT_CSV_PATH = config.has("baseOutputCsvPath")
  ? config.get("baseOutputCsvPath")
  : DEFAULT_BASE_OUTPUT_CSV_PATH;
const ADDRESS_COLUMN_INDEX = config.has("addressColumnIndex")
  ? config.get("addressColumnIndex")
  : DEFAULT_ADDRESS_COLUMN_INDEX;
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY; // From .env
const REQUEST_DELAY_MS = config.has("requestDelayMs")
  ? config.get("requestDelayMs")
  : DEFAULT_REQUEST_DELAY_MS;
const MAX_RECORDS_TO_PROCESS = config.has("maxRecordsToProcess")
  ? config.get("maxRecordsToProcess")
  : DEFAULT_MAX_RECORDS_TO_PROCESS;
const BATCH_WRITE_SIZE = config.has("batchWriteSize")
  ? config.get("batchWriteSize")
  : DEFAULT_BATCH_WRITE_SIZE;

const FROM_ROW = config.has("fromRow")
  ? config.get("fromRow")
  : DEFAULT_FROM_ROW;
const LOGS_FOLDER = config.has("logsFolder")
  ? config.get("logsFolder")
  : DEFAULT_LOGS_FOLDER;

// --- Global for Logging ---
let LOG_FILE_PATH = path.join(__dirname, LOGS_FOLDER, "gs_geocode_csv.log"); // Default log file path

/**
 * Displays help information and exits.
 */
function displayHelp() {
  console.log(`
CSV Geocoding Tool - by Gustavo Adrián Salvini

This script reads a CSV file, geocodes addresses from a specified column, optionally
starts processing from a specific row, to retrieve latitude and longitude using
Google Geocoding API, and then writes a new CSV file with these additional columns.

It supports incremental saving and can resume processing from a specific row.
A detailed log file is also generated for each run.

## Usage:

node gs_geocode_csv.js [--help] [--from-row <number>]
bun gs_geocode_csv.js [--help] [--from-row <number>]

### Options:

  --help                    Show this help message and exit.

  --from-row=NUMBER         Optional. The row number (1-based) in the input CSV file
                            to start processing from. If specified, the output filename
                            will have a suffix like "-from-row-NUMBER.csv".
                            Example: --from-row=50

## Setup:

  1. Install dependencies:
     "npm install" or "bun install"
  2. Create a ".env" file in the same directory as this script for the API key:
     GOOGLE_API_KEY=YOUR_ACTUAL_API_KEY_HERE
  3. (Optional) Create a config/default.json file to customize settings. Example content:

  {
    "inputCsvPath": "path/to/my_input.csv",
    "baseOutputCsvPath": "path/to/my_output.csv",
    "logsFolder": "logs",
    "addressColumnIndex": 9,
    "requestDelayMs": 200,
    "maxRecordsToProcess": 0,
    "batchWriteSize": 20,
    "fromRow": 0
  }

  If config/default.json is not found or a setting is missing, internal defaults are used.

## Log File:

A log file named "gs_geocode_csv_{PROCESS_START_TIMESTAMP}.log" will be created in the
script's directory. It contains details of script progress and, for geocoding issues
(e.g., invalid address, API errors), it will include the full data of the affected record.

## Default Configuration Values (used if not set in config/default.json):

  Input CSV Path:         "${DEFAULT_INPUT_CSV_PATH}"
  Base Output CSV Path:   "${DEFAULT_BASE_OUTPUT_CSV_PATH}"
  Logs Folder:            "${DEFAULT_LOGS_FOLDER}"
  Address Column Index:   ${DEFAULT_ADDRESS_COLUMN_INDEX} (0-indexed, so the ${DEFAULT_ADDRESS_COLUMN_INDEX + 1}th column)
  API Request Delay:      ${DEFAULT_REQUEST_DELAY_MS}ms
  Max Records to Process: ${DEFAULT_MAX_RECORDS_TO_PROCESS} (0 means no limit for the run)
  Batch Write Size:       ${DEFAULT_BATCH_WRITE_SIZE}
  From Row:               ${DEFAULT_FROM_ROW}

## Examples:

  "node gs_geocode_csv.js" or "bun gs_geocode_csv.js"
  "node gs_geocode_csv.js --from-row=101" or "bun gs_geocode_csv.js --from-row=101"
  `);
  process.exit(0);
}

/**
 * Delays execution for a specified number of milliseconds.
 * @param {number} ms The number of milliseconds to wait.
 * @returns {Promise<void>} A promise that resolves after the delay.
 */
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Asynchronously geocodes an address string using Google Geocoding API.
 * @param {string} address The address to geocode.
 * @returns {Promise<{latitude: string|number, longitude: string|number, statusMessage?: string}>}
 */
async function geocodeAddress(address) {
  if (!address || String(address).trim() === "") {
    return {
      latitude: "",
      longitude: "",
      statusMessage: "Empty or invalid address",
    };
  }
  if (
    !GOOGLE_API_KEY ||
    GOOGLE_API_KEY === "YOUR_GOOGLE_MAPS_API_KEY" ||
    GOOGLE_API_KEY.length < 10
  ) {
    const msg =
      "Google API Key is not set or is invalid. Please ensure it is correctly set in your .env file (GOOGLE_API_KEY=YOUR_ACTUAL_KEY).";
    // This specific message will be logged as ERROR type from the main loop if API key is missing.
    return {
      latitude: "API_KEY_MISSING",
      longitude: "API_KEY_MISSING",
      statusMessage: msg,
    };
  }
  const encodedAddress = encodeURIComponent(address);
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodedAddress}&key=${GOOGLE_API_KEY}`;
  try {
    const response = await axios.get(url);
    if (
      response.data &&
      response.data.results &&
      response.data.results.length > 0
    ) {
      const location = response.data.results[0].geometry.location;
      return {
        latitude: location.lat,
        longitude: location.lng,
        statusMessage: "Success",
      };
    }
    const status = response.data.status;
    const errorMessage =
      response.data.error_message || "No additional error message.";
    const detailedStatusMessage = `API Status: ${status}. Details: ${errorMessage}`;
    // Console warning for geocoding failures is now handled in the main loop if it's a "FAIL" type.
    // console.warn(`[Geocoding Failure] Address: "${address}" - ${detailedStatusMessage}`);
    if (status === "ZERO_RESULTS")
      return {
        latitude: "Not Found",
        longitude: "Not Found",
        statusMessage: detailedStatusMessage,
      };
    if (status === "OVER_QUERY_LIMIT")
      return {
        latitude: "Rate Limit Error",
        longitude: "Rate Limit Error",
        statusMessage: detailedStatusMessage,
      };
    return {
      latitude: "API Error",
      longitude: "API Error",
      statusMessage: detailedStatusMessage,
    };
  } catch (error) {
    const detailedStatusMessage = `Request failed: ${error.message}`;
    // Console error for geocoding errors is now handled in the main loop if it's a "FAIL" type.
    // console.error(`[Geocoding Error] Address: "${address}" - ${detailedStatusMessage}`);
    return {
      latitude: "Request Error",
      longitude: "Request Error",
      statusMessage: detailedStatusMessage,
    };
  }
}

/**
 * Writes a batch of log messages to the log file, ensuring data is flushed.
 * @param {string} logFilePath Path to the log file.
 * @param {Array<string>} messagesArray Array of log messages.
 */
async function writeBatchToLogFile(logFilePath, messagesArray) {
  if (messagesArray.length === 0) return;
  const logString = messagesArray.join("\n") + "\n";
  let fd;
  try {
    // Ensure the directory exists
    const logDir = path.dirname(logFilePath);
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    fs.appendFileSync(logFilePath, logString);
    fd = fs.openSync(logFilePath, "r+");
    fs.fsyncSync(fd);
  } catch (error) {
    console.error(
      `[Log File Error] Error appending to or fsyncing log file ${logFilePath}:`,
      error,
    );
  } finally {
    if (fd !== undefined) {
      try {
        fs.closeSync(fd);
      } catch (e) {
        console.error(
          `[Log File Error] Error closing log file descriptor: ${e.message}`,
        );
      }
    }
  }
}

/**
 * Helper function to write a batch of data to a CSV file.
 * @param {string} filePath Path to the CSV file.
 * @param {Array<Object>} dataArray Array of row objects to write.
 * @param {Array<string>} headersArray Array of header strings.
 * @param {boolean} shouldWriteHeader True if headers should be written (first batch), false otherwise.
 * @returns {Promise<void>}
 */
async function writeBatchToCsv(
  filePath,
  dataArray,
  headersArray,
  shouldWriteHeader,
) {
  if (shouldWriteHeader) {
    return new Promise((resolve, reject) => {
      const fileStream = fs.createWriteStream(filePath, { flags: "w" });
      fileStream.on("error", (err) => {
        console.error(
          `[CSV Write Error] File stream error (write/overwrite) for ${filePath}:`,
          err,
        );
        reject(err);
      });
      const csvStream = fastcsv.format({
        headers: headersArray,
        writeHeaders: true,
        includeEndRowDelimiter: true,
      });
      csvStream
        .pipe(fileStream)
        .on("finish", resolve)
        .on("error", (err) => {
          console.error(
            "[CSV Write Error] CSV stream or pipe error (write/overwrite):",
            err,
          );
          reject(err);
        });
      dataArray.forEach((row) => {
        if (!csvStream.writableEnded) csvStream.write(row);
      });
      if (!csvStream.writableEnded) csvStream.end();
    });
  } else {
    if (dataArray.length === 0) return Promise.resolve();
    let fd;
    try {
      const csvString = await fastcsv.writeToString(dataArray, {
        headers: false,
        writeHeaders: false,
        includeEndRowDelimiter: true,
      });
      if (csvString && csvString.trim() !== "") {
        fs.appendFileSync(filePath, csvString);
        fd = fs.openSync(filePath, "r+");
        fs.fsyncSync(fd);
      }
      return Promise.resolve();
    } catch (error) {
      console.error(
        `[CSV Write Error] Error preparing, appending, or fsyncing batch to ${filePath}:`,
        error,
      );
      return Promise.reject(error);
    } finally {
      if (fd !== undefined) {
        try {
          fs.closeSync(fd);
        } catch (e) {
          console.error(
            `[CSV Write Error] Error closing CSV file descriptor: ${e.message}`,
          );
        }
      }
    }
  }
}

/**
 * Main function to process the CSV file.
 */
async function processCSV(fromRow = FROM_ROW) {
  if (process.argv.includes("-h") || process.argv.includes("--help")) {
    displayHelp();
  }

  const processStartTimeISO = new Date().toISOString();
  const processStartTimeFileFriendly = processStartTimeISO.replace(
    /[:.]/g,
    "-",
  );
  // Ensure logs folder exists
  const logsFolderPath = path.join(__dirname, LOGS_FOLDER);
  if (!fs.existsSync(logsFolderPath)) {
    fs.mkdirSync(logsFolderPath, { recursive: true });
  }
  LOG_FILE_PATH = path.join(logsFolderPath, `gs_geocode_csv_${processStartTimeFileFriendly}.log`);
  console.log(`[INFO] Logging to: ${LOG_FILE_PATH}`);
  await writeBatchToLogFile(LOG_FILE_PATH, [
    `[${processStartTimeISO}] Script started. CLI arguments: ${process.argv.slice(2).join(" ")}`,
  ]);

  if (
    !GOOGLE_API_KEY ||
    GOOGLE_API_KEY === "YOUR_GOOGLE_MAPS_API_KEY" ||
    GOOGLE_API_KEY.length < 10
  ) {
    const errorMsg =
      "ERROR: Google API Key is not configured. Please create a .env file and add GOOGLE_API_KEY=YOUR_KEY.";
    console.error(`[FATAL] ${errorMsg}`);
    await writeBatchToLogFile(LOG_FILE_PATH, [
      `[${new Date().toISOString()}] FATAL: ${errorMsg}`,
    ]);
    console.error("[INFO] Run with -h or --help for more instructions.");
    process.exit(1);
  }

  let actualOutputCsvPath = BASE_OUTPUT_CSV_PATH;
  let startRow = 0;

  const fromRowArg = process.argv.find((arg) => arg.startsWith("--from-row="));
  if (fromRowArg) {
    const fromRowValue = parseInt(fromRowArg.split("=")[1], 10);
    if (!isNaN(fromRowValue) && fromRowValue > 0) {
      startRow = fromRowValue - 1;
      const outputDir = path.dirname(BASE_OUTPUT_CSV_PATH);
      const outputFileName = path.basename(BASE_OUTPUT_CSV_PATH, ".csv");
      const outputFileExt = ".csv";
      actualOutputCsvPath = path.join(
        outputDir,
        `${outputFileName}-from-row-${fromRowValue}${outputFileExt}`,
      );
      const msg = `Processing will start from input row: ${fromRowValue} (0-indexed data row ${startRow}). Output: ${actualOutputCsvPath}`;
      console.log(`[INFO] ${msg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${msg}`,
      ]);
    } else {
      const msg = `Invalid --from-row value: "${fromRowArg.split("=")[1]}". Processing all rows. Output: ${actualOutputCsvPath}`;
      console.warn(`[WARN] ${msg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] WARN: ${msg}`,
      ]);
    }
  } else {
    const msg = `No --from-row specified. Processing all rows. Output: ${actualOutputCsvPath}`;
    console.log(`[INFO] ${msg}`);
    await writeBatchToLogFile(LOG_FILE_PATH, [
      `[${new Date().toISOString()}] INFO: ${msg}`,
    ]);
  }

  const rows = [];
  let originalHeaders = [];

  const initialConfigMsg = `Effective Configuration:
  Input CSV Path:         ${INPUT_CSV_PATH}
  Base Output CSV Path:   ${BASE_OUTPUT_CSV_PATH} (Actual for this run: ${actualOutputCsvPath})
  Address Column Index:   ${ADDRESS_COLUMN_INDEX}
  API Request Delay:      ${REQUEST_DELAY_MS}ms
  Max Records to Process: ${MAX_RECORDS_TO_PROCESS === 0 ? "All applicable" : MAX_RECORDS_TO_PROCESS}
  Batch Write Size:       ${BATCH_WRITE_SIZE}`;
  console.log(`[INFO] ${initialConfigMsg.replace(/\n/g, "\n[INFO] ")}`); // Indent multi-line log
  await writeBatchToLogFile(LOG_FILE_PATH, [
    `[${new Date().toISOString()}] INFO: ${initialConfigMsg.replace(/\n/g, " ")}`,
  ]);

  const readStream = fs.createReadStream(INPUT_CSV_PATH);

  readStream
    .on("error", async (error) => {
      const errorMsg = `Error reading input CSV file "${INPUT_CSV_PATH}": ${error.message}`;
      console.error(`[ERROR] ${errorMsg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] ERROR: ${errorMsg}`,
      ]);
      try {
        if (
          !fs.existsSync(actualOutputCsvPath) ||
          fs.statSync(actualOutputCsvPath).size === 0
        ) {
          fs.writeFileSync(
            actualOutputCsvPath,
            `Error reading input CSV: ${error.message}`,
          );
        }
      } catch (writeError) {
        console.error(
          `[ERROR] Additionally, failed to write error to output file path: ${writeError.message}`,
        );
      }
    })
    .pipe(csvParser())
    .on("headers", (h) => {
      originalHeaders = [...h];
      const headerMsg = `Detected headers: ${originalHeaders.join(", ")}`;
      console.log(`[INFO] ${headerMsg}`);
      writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${headerMsg}`,
      ]);
    })
    .on("data", (row) => {
      rows.push(row);
    })
    .on("end", async () => {
      const readEndMsg = `CSV file reading complete. Total data rows read into memory: ${rows.length}.`;
      console.log(`[INFO] ${readEndMsg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${readEndMsg}`,
      ]);

      const headersWithoutExistingGeo = originalHeaders.filter(
        (h) =>
          h.toLowerCase() !== "latitude" && h.toLowerCase() !== "longitude",
      );
      const newHeaders = [
        ...headersWithoutExistingGeo,
        "Latitude",
        "Longitude",
      ];

      if (rows.length === 0 && originalHeaders.length === 0) {
        const msg =
          "Input CSV appears to be empty or no headers were found. Ensure CSV is valid.";
        console.log(`[WARN] ${msg}`);
        await writeBatchToLogFile(LOG_FILE_PATH, [
          `[${new Date().toISOString()}] WARN: ${msg}`,
        ]);
        try {
          if (
            !fs.existsSync(actualOutputCsvPath) ||
            fs.statSync(actualOutputCsvPath).size === 0
          ) {
            await fsp.writeFile(
              actualOutputCsvPath,
              "Input CSV empty or no headers found.",
            );
          }
        } catch (e) {
          console.error("[ERROR] Error writing empty/error file:", e);
        }
        return;
      } else if (rows.length === 0 && originalHeaders.length > 0) {
        const msg =
          "Input CSV has headers but no data rows. Writing output with new headers and no data.";
        console.log(`[INFO] ${msg}`);
        await writeBatchToLogFile(LOG_FILE_PATH, [
          `[${new Date().toISOString()}] INFO: ${msg}`,
        ]);
        try {
          await writeBatchToCsv(actualOutputCsvPath, [], newHeaders, true);
          console.log(
            `[INFO] Output CSV "${actualOutputCsvPath}" created with headers and no data.`,
          );
        } catch (err) {
          console.error("[ERROR] Error writing CSV with headers only:", err);
          await writeBatchToLogFile(LOG_FILE_PATH, [
            `[${new Date().toISOString()}] ERROR writing CSV with headers only: ${err.message}`,
          ]);
        }
        return;
      }

      if (originalHeaders.length <= ADDRESS_COLUMN_INDEX) {
        const errorMsg = `Error: The CSV file does not have enough columns. Expected at least ${ADDRESS_COLUMN_INDEX + 1} for address, found ${originalHeaders.length}.`;
        console.error(`[ERROR] ${errorMsg}`);
        await writeBatchToLogFile(LOG_FILE_PATH, [
          `[${new Date().toISOString()}] ERROR: ${errorMsg}`,
        ]);
        try {
          if (
            !fs.existsSync(actualOutputCsvPath) ||
            fs.statSync(actualOutputCsvPath).size === 0
          ) {
            fs.writeFileSync(actualOutputCsvPath, errorMsg);
          }
        } catch (e) {
          console.error("[ERROR] Error writing column error file:", e);
        }
        return;
      }
      const addressHeaderName = originalHeaders[ADDRESS_COLUMN_INDEX];
      const columnMsg = `Using column "${addressHeaderName}" (index ${ADDRESS_COLUMN_INDEX}) for addresses.`;
      console.log(`[INFO] ${columnMsg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${columnMsg}`,
      ]);

      let headersHaveBeenWritten = false;
      if (
        fs.existsSync(actualOutputCsvPath) &&
        fs.statSync(actualOutputCsvPath).size > 0
      ) {
        const msg = `Output file ${actualOutputCsvPath} exists and is not empty. Assuming headers are present. Will append new data.`;
        console.log(`[INFO] ${msg}`);
        await writeBatchToLogFile(LOG_FILE_PATH, [
          `[${new Date().toISOString()}] INFO: ${msg}`,
        ]);
        headersHaveBeenWritten = true;
      } else {
        const msg = `Output file ${actualOutputCsvPath} does not exist or is empty. Headers will be written with the first batch.`;
        console.log(`[INFO] ${msg}`);
        await writeBatchToLogFile(LOG_FILE_PATH, [
          `[${new Date().toISOString()}] INFO: ${msg}`,
        ]);
      }

      let processedRowsBatch = [];
      let logMessagesBatch = [];
      let geocodedCountInThisRun = 0;

      const totalRowsToConsiderForGeocoding = rows.length - startRow;
      const geocodingStartMsg = `Starting geocoding. Will process rows from input index ${startRow} onwards. Total applicable rows in input: ${totalRowsToConsiderForGeocoding}.`;
      console.log(`[INFO] ${geocodingStartMsg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${geocodingStartMsg}`,
      ]);

      for (let i = 0; i < rows.length; i++) {
        const currentRowInInputFile = i + 1;

        if (i < startRow) {
          continue;
        }

        const row = rows[i];
        const address = row[addressHeaderName];

        let geocodedDataResult;
        let lat = "",
          lon = "";

        if (
          MAX_RECORDS_TO_PROCESS > 0 &&
          geocodedCountInThisRun >= MAX_RECORDS_TO_PROCESS
        ) {
          geocodedDataResult = {
            latitude: "",
            longitude: "",
            statusMessage: `Skipped due to MAX_RECORDS_TO_PROCESS limit (${MAX_RECORDS_TO_PROCESS}) reached for this run.`,
          };
        } else if (address && String(address).trim() !== "") {
          try {
            geocodedDataResult = await geocodeAddress(String(address));
            lat = geocodedDataResult.latitude;
            lon = geocodedDataResult.longitude;
          } catch (geoError) {
            const errorMsg = `Geocoding function threw an error for address "${address}" (input row ${currentRowInInputFile}): ${geoError.message}`;
            // console.error(`[ERROR] ${errorMsg}`); // This is now handled by the FAIL log logic below for console
            geocodedDataResult = {
              latitude: "Geocoding Error",
              longitude: "Geocoding Error",
              statusMessage: errorMsg,
            };
            lat = geocodedDataResult.latitude;
            lon = geocodedDataResult.longitude;
          }
        } else {
          geocodedDataResult = {
            latitude: "",
            longitude: "",
            statusMessage: "Empty or invalid address.",
          }; // Added a period for consistency
        }

        const isSkippedByLimit =
          geocodedDataResult.statusMessage ===
          `Skipped due to MAX_RECORDS_TO_PROCESS limit (${MAX_RECORDS_TO_PROCESS}) reached for this run.`;
        const currentTimestamp = new Date().toISOString();

        if (
          geocodedDataResult.statusMessage !== "Success" &&
          !isSkippedByLimit
        ) {
          let logType = "FAIL";
          let failureReason = geocodedDataResult.statusMessage;

          if (
            geocodedDataResult.statusMessage === "Empty or invalid address."
          ) {
            logType = "INFO";
            failureReason = "Skipped - Address field is empty or invalid.";
            console.warn(
              `[WARN] Input row ${currentRowInInputFile}: Address field is empty or invalid. Skipping geocoding for this record.`,
            );
          } else if (
            geocodedDataResult.statusMessage.includes("API_KEY_MISSING")
          ) {
            logType = "ERROR";
            failureReason = geocodedDataResult.statusMessage;
            // Console error for API Key is already at the start and in geocodeAddress if it returns this.
          }
          // For other true failures (API Error, Not Found, Rate Limit, Request Error),
          // geocodeAddress already prints a console.warn/error.
          // The following ensures the full record is also on console for "FAIL" types.

          const logEntryContent = `[Input Row: ${currentRowInInputFile}] Status: ${failureReason} Address: "${address || "(empty)"}". Record Data: ${JSON.stringify(row)}`;
          const fullLogEntryForFile = `[${currentTimestamp}] ${logType}: ${logEntryContent}`;
          logMessagesBatch.push(fullLogEntryForFile);

          if (logType === "FAIL") {
            console.error(`[FAIL] ${logEntryContent}`); // Mirror FAIL entry to console with full data
          }
        }

        if (
          address &&
          String(address).trim() !== "" &&
          !(
            MAX_RECORDS_TO_PROCESS > 0 &&
            geocodedCountInThisRun >= MAX_RECORDS_TO_PROCESS
          )
        ) {
          geocodedCountInThisRun++;
        }

        const outputRow = { ...row };
        outputRow.Latitude = lat;
        outputRow.Longitude = lon;
        processedRowsBatch.push(outputRow);

        const isLastRowOfInput = i === rows.length - 1;
        const maxRecordsProcessedThisRun =
          MAX_RECORDS_TO_PROCESS > 0 &&
          geocodedCountInThisRun >= MAX_RECORDS_TO_PROCESS &&
          address &&
          String(address).trim() !== "";

        if (
          processedRowsBatch.length >= BATCH_WRITE_SIZE ||
          isLastRowOfInput ||
          maxRecordsProcessedThisRun
        ) {
          if (processedRowsBatch.length > 0) {
            try {
              console.log(
                `[INFO] Writing batch of ${processedRowsBatch.length} rows to CSV. Input rows up to ${currentRowInInputFile}. (Batch #${Math.ceil(geocodedCountInThisRun / BATCH_WRITE_SIZE) || 1})`,
              );
              const shouldWriteHeadersForThisBatch = !headersHaveBeenWritten;
              await writeBatchToCsv(
                actualOutputCsvPath,
                processedRowsBatch,
                newHeaders,
                shouldWriteHeadersForThisBatch,
              );

              if (shouldWriteHeadersForThisBatch) {
                headersHaveBeenWritten = true;
              }

              if (logMessagesBatch.length > 0) {
                await writeBatchToLogFile(LOG_FILE_PATH, logMessagesBatch);
                logMessagesBatch = [];
              }
              processedRowsBatch = [];
            } catch (writeError) {
              const errorMsg = `Error writing batch to CSV (input rows up to ${currentRowInInputFile}): ${writeError.message}. Data in this batch may be lost.`;
              console.error(`[ERROR] ${errorMsg}`);
              await writeBatchToLogFile(LOG_FILE_PATH, [
                `[${new Date().toISOString()}] ERROR: ${errorMsg}`,
              ]);
            }
          }
          if (maxRecordsProcessedThisRun) {
            const limitMsg = `MAX_RECORDS_TO_PROCESS (${MAX_RECORDS_TO_PROCESS}) limit hit. Ending geocoding for this run.`;
            console.log(`[INFO] ${limitMsg}`);
            await writeBatchToLogFile(LOG_FILE_PATH, [
              `[${new Date().toISOString()}] INFO: ${limitMsg}`,
            ]);
            break;
          }
        }

        if (
          geocodedDataResult.statusMessage === "Success" &&
          REQUEST_DELAY_MS > 0
        ) {
          let isLastToBeGeocodedInThisRun = false;
          if (
            MAX_RECORDS_TO_PROCESS > 0 &&
            geocodedCountInThisRun >= MAX_RECORDS_TO_PROCESS
          ) {
            isLastToBeGeocodedInThisRun = true;
          } else if (i === rows.length - 1) {
            isLastToBeGeocodedInThisRun = true;
          }

          if (!isLastToBeGeocodedInThisRun) {
            await delay(REQUEST_DELAY_MS);
          }
        }
      }
      const loopEndMsg = `Processing loop finished. Output saved to "${actualOutputCsvPath}". Total rows from input: ${rows.length}. Records geocoded in this run: ${geocodedCountInThisRun}.`;
      console.log(`[INFO] ${loopEndMsg}`);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] INFO: ${loopEndMsg}`,
      ]);
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] Script finished.`,
      ]);
    })
    .on("close", () => {
      /* console.log('Input CSV file stream closed.'); */
    });
}

// Run the main processing function
processCSV().catch(async (err) => {
  const errorMsg = `Unhandled error in the CSV processing script: ${err.message}\n${err.stack}`;
  console.error(`[FATAL] ${errorMsg}`);
  if (LOG_FILE_PATH) {
    try {
      await writeBatchToLogFile(LOG_FILE_PATH, [
        `[${new Date().toISOString()}] FATAL ERROR: ${errorMsg}`,
      ]);
    } catch (logErr) {
      console.error(
        "[FATAL] Additionally, failed to write fatal error to log file:",
        logErr,
      );
    }
  }
  console.error(
    `[INFO] Consider checking output file for partially saved data.`,
  );
});
