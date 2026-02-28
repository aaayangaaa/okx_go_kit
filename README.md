# okx_go_kit

A high-performance Go toolkit for downloading and converting OKX historical market data (Level-2 order book snapshots and trade records) into Parquet format for efficient offline analysis.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Flags](#flags)
  - [Examples](#examples)
- [Output Structure](#output-structure)
- [Data Format Reference](#data-format-reference)
  - [Order Book (L2)](#order-book-l2)
  - [Trades](#trades)
- [Architecture & Design](#architecture--design)
- [Error Handling & Retry Logic](#error-handling--retry-logic)
- [Performance Notes](#performance-notes)
- [Dependencies](#dependencies)
- [License](#license)

---

## Overview

`okx_go_kit` automates the retrieval of historical tick-by-tick market data from OKX's public broker data endpoint. It supports:

- **L2 Order Book** snapshots (NDJSON format compressed in `.tar.gz`)
- **Trade records** (CSV format compressed in `.zip` or `.csv.gz`)

All downloaded data is converted and saved as **Snappy-compressed Parquet files**, making them directly consumable by tools such as DuckDB, Pandas, Polars, Apache Spark, and others.

---

## Project Structure

```
okx_go_kit/
├── okx_historical_L2_trade.go   # Main source file
├── go.mod                       # Go module definition
├── go.sum                       # Dependency checksums (auto-generated)
└── okx_data/
    ├── raw_tmp/                 # Temporary download cache (auto-cleaned)
    └── parquet/                 # Final Parquet output files
```

---

## Prerequisites

- **Go 1.21+** — [https://go.dev/dl/](https://go.dev/dl/)
- Network access to `www.okx.com`

---

## Installation

```bash
# Clone or copy the project
cd okx_go_kit

# Download dependencies
go mod tidy

# Build the binary
go build -o okx_go_kit .
```

---

## Usage

```bash
./okx_L2_trade [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-mode` | string | `all` | Download mode: `trade`, `L2`, or `all` |
| `-date` | string | today | Start date in `YYYY-MM-DD` format (inclusive) |
| `-days` | int | `7` | Number of days to look back from `-date` |
| `-symbol` | string | `ETH-USDT-SWAP` | Trading pair (see Symbol Format below) |

#### Symbol Format

The symbol argument follows OKX's naming convention:

| Symbol | Instrument Type | Notes |
|--------|----------------|-------|
| `ETH-USDT-SWAP` | Perpetual Swap | Most common for L2 data |
| `BTC-USDT-SWAP` | Perpetual Swap | |
| `BTC-USDT` | Spot | |
| `ETH-USD-PERP` | USD-margined Perpetual | |

The last segment after the final `-` is parsed as `instType` (e.g. `SWAP`, `SPOT`), and everything before it becomes `instFamily` (e.g. `ETH-USDT`).

---

### Examples

**Download last 7 days of both L2 and trade data for ETH-USDT-SWAP (default):**
```bash
./okx_go_kit
```

**Download only L2 order book data for the last 30 days:**
```bash
./okx_go_kit -mode L2 -days 30
```

**Download trade data for BTC-USDT-SWAP, starting from a specific date:**
```bash
./okx_go_kit -mode trade -date 2024-06-01 -days 14 -symbol BTC-USDT-SWAP
```

**Download all data types for a spot pair:**
```bash
./okx_go_kit -mode all -date 2024-01-15 -days 5 -symbol BTC-USDT
```

---

## Output Structure

Downloaded and converted files are stored under `./okx_data/parquet/`. Each file corresponds to one raw archive from OKX and follows the original filename convention with a `.parquet` extension appended.

```
okx_data/
└── parquet/
    ├── BTC-USDT-SWAP_l2_orderbook_2024-06-01.tar.gz.parquet
    ├── BTC-USDT-SWAP_trades_2024-06-01.zip.parquet
    └── ...
```

> Already-converted files are automatically skipped on subsequent runs, making the tool safe to re-run incrementally.

---

## Data Format Reference

### Order Book (L2)

Parsed from NDJSON entries inside `.tar.gz` archives. Each row in the output Parquet represents a single price level from a snapshot update.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `int64` | Timestamp in Unix milliseconds (UTC) |
| `action` | `string` | Update type: `snapshot` or `update` |
| `side` | `string` | `asks` or `bids` |
| `px` | `float32` | Price level |
| `sz` | `float32` | Size / quantity at this price level |
| `orders` | `int32` | Number of orders at this level (may be 0 if not provided) |

**Raw NDJSON example (one line):**
```json
{
  "ts": 1717200000000,
  "action": "snapshot",
  "asks": [["65000.0", "1.5", "3"], ["65001.0", "0.8", "1"]],
  "bids": [["64999.0", "2.0", "5"], ["64998.0", "1.2", "2"]]
}
```

---

### Trades

Parsed from CSV files inside `.zip` or `.csv.gz` archives. Column names are preserved as-is from OKX's original headers.

Typical columns include:

| Column | Description |
|--------|-------------|
| `ts` or `timestamp` | Trade timestamp (Unix ms) |
| `side` | `buy` or `sell` |
| `px` or `price` | Trade price |
| `sz` or `size` | Trade size |

> Column availability depends on the instrument type and OKX's data version. All values are stored as strings in the Parquet schema to ensure forward compatibility.

---

## Architecture & Design

```
main()
  └── runSync()
        ├── fetchDownloadLinks()    # POST to OKX API → list of file URLs
        ├── downloadFile()          # GET file → save to raw_tmp/
        └── extractAndConvert()
              ├── .zip             → parseCSVToParquet()
              ├── .tar.gz          → parseOrderbookNDJSON()  (if NDJSON)
              │                  → parseCSVToParquet()       (if CSV)
              └── .csv.gz          → parseCSVToParquet()
```

**Key design decisions:**

- **Streaming I/O**: All decompression and parsing is done in streaming fashion — no full file is loaded into memory at once.
- **Chunked writes**: CSV rows are accumulated in batches of 300,000 before being flushed to Parquet. NDJSON order book rows are batched at 100,000 rows.
- **Snappy compression**: All output Parquet files use Snappy compression, balancing speed and file size.
- **Idempotent runs**: The tool checks for existing output files before downloading, allowing safe re-execution.
- **Typed schema for L2**: Order book rows use a strongly-typed Go struct (`int64`, `float32`, `int32`) for compact, query-efficient Parquet output.

---

## Error Handling & Retry Logic

All network operations (API calls and file downloads) implement automatic retry with linear backoff:

- **Maximum retries**: 3 attempts per operation
- **Retry delay**: `attempt × 2 seconds` (2s, 4s, 6s)
- **HTTP 404**: Immediately returns `false` without retrying (file genuinely missing)
- **Empty file**: Detected post-download and treated as a failure

At the end of each run, a summary is printed:

```
Complete: Success 14, Skip 0, Fail 0
```

| Status | Meaning |
|--------|---------|
|  (Success) | Downloaded and converted successfully |
|  (Skipped) | Parquet file already exists, skipped |
|  (Failed) | API returned no links, download failed, or conversion produced 0 rows |

---

## Performance Notes

| Aspect | Detail |
|--------|--------|
| Concurrency | Sequential per-task (safe for API rate limits); file I/O is streaming |
| Memory usage | Bounded by chunk size (~300K CSV rows or ~100K order book rows in memory at a time) |
| Disk usage | Raw archives in `raw_tmp/` are deleted immediately after successful conversion |
| L2 Parquet size | Typically 5–20% of original `.tar.gz` size due to typed schema + Snappy |
| Trade Parquet size | Typically 40–60% of original `.zip` size |

---

## Dependencies

| Package | Purpose |
|---------|---------|
| [`github.com/parquet-go/parquet-go`](https://github.com/parquet-go/parquet-go) | Parquet file writing (typed & generic schema) |
| [`github.com/schollz/progressbar/v3`](https://github.com/schollz/progressbar) | Terminal progress bar |

All other functionality uses Go's standard library (`archive/zip`, `archive/tar`, `compress/gzip`, `encoding/csv`, `encoding/json`, `net/http`).

Install all dependencies with:

```bash
go mod tidy
```

---

## License

This project is provided as-is for personal and research use. OKX historical data is subject to [OKX's Terms of Service](https://www.okx.com/legal/terms-of-service.html). Ensure compliance before using data for commercial purposes.
