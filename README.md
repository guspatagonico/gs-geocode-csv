# GS Geocode Processor

GS Geocode Processor is a high-performance geocoding tool built with the [Bun](https://bun.sh/) runtime.
It efficiently processes geocoding tasks by reading addresses from a CSV file, sending them to a geocoding API, and writing the results back to an output CSV file. Leveraging Bun's speed and modern features, this tool is optimized for performance and developer experience.

As of the date of writing (May 14th, 2025), it is still compatible with Node.js Runtime, but I'm exploring to make it more Bun-friendly as an excuse to learn more about Bun.

---

[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white&style=for-the-badge)](https://conventionalcommits.org)

[![Commitizen friendly](https://img.shields.io/badge/commitizen-friendly-brightgreen.svg?style=for-the-badge)](http://commitizen.github.io/cz-cli/)

---

## Features

- **Powered by Bun**: Built on the blazing-fast Bun runtime for superior performance.
- **CSV Input/Output**: Reads addresses from a CSV file and writes geocoded results to another CSV file.
- **Batch Processing**: Processes records in configurable batch sizes for efficiency.
- **Rate Limiting**: Configurable delay between API requests to avoid hitting rate limits.
- **Customizable**: Easily configurable via a JSON configuration file.
- **Error Handling**: Handles errors gracefully and logs issues for debugging.

## Configuration

The tool uses a configuration file (`config/default.json`) to define its behavior. Below is an example configuration:

```json
{
  "inputCsvPath": "input/input.csv",
  "baseOutputCsvPath": "output/output.csv",
  "addressColumnIndex": 9,
  "requestDelayMs": 200,
  "maxRecordsToProcess": 0,
  "batchWriteSize": 10,
  "fromRow": 0,
  "logsFolder": "logs"
}
```

### Configuration Options

- **`inputCsvPath`**: Path to the input CSV file containing addresses.
- **`baseOutputCsvPath`**: Path to the base output CSV file where results will be written.
- **`addressColumnIndex`**: The column index (0-based) in the input CSV that contains the address data.
- **`requestDelayMs`**: Delay (in milliseconds) between API requests to avoid rate limits.
- **`maxRecordsToProcess`**: Maximum number of records to process. Set to `0` to process all records.
- **`batchWriteSize`**: Number of records to write to the output file in each batch.
- **`fromRow`**: Number of record to start processing from.
- **`logsFolder`**: Directory where log files will be stored. Defaults to `"logs"`.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/guspatagonico/gs-geocode-csv.git
   cd gs-geocode-csv
   ```

2. Install dependencies using Bun or Npm:
   ```bash
   bun install
   ```

or

   ```bash
   npm install
   ```

## Why Bun?

Bun is a modern JavaScript runtime that offers several advantages over traditional runtimes like Node.js:
- **Speed**: Bun is designed to be fast, with a focus on performance.
- **Built-in Tools**: Includes a bundler, transpiler, and task runner out of the box.
- **Modern APIs**: Supports modern JavaScript and TypeScript features seamlessly.

By leveraging Bun, GS Geocode Processor ensures a faster and smoother experience for developers and users alike.

I have nothing against Node.js, but I'm leveraging this project to explore Bun and learn more about it.

## Requirements

- [Bun](https://bun.sh/) (v1.0 or later) (Node.js is also supported if you prefer)

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes and push them to your fork.
4. Submit a pull request with a detailed description of your changes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author

This project was created and is maintained by **Gustavo Adrián Salvini**.

## Contact Information

- **Personal website**: [https://gustavosalvini.com.ar](https://gustavosalvini.com.ar)
- **GitHub**: [https://github.com/guspatagonico/](https://github.com/guspatagonico/)
- **X**: [@guspatagonico](https://twitter.com/guspatagonico)
- **Mastodon**: [https://techhub.social/@guspatagonico](https://techhub.social/@guspatagonico)
