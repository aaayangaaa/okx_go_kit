package main

import (
    "archive/tar"
    "archive/zip"
    "bytes"
    "compress/gzip"
    "encoding/csv"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/parquet-go/parquet-go"
    "github.com/schollz/progressbar/v3"
)

// ─────────────────────────────────────────
// Global Setting
// ─────────────────────────────────────────

const (
    BaseDir    = "./okx_data"
    RawDir     = BaseDir + "/raw_tmp"
    ParquetDir = BaseDir + "/parquet"
    MaxRetry   = 3
    RetryDelay = 2 * time.Second
    API        = "https://www.okx.com/priapi/v5/broker/public/trade-data/download-link"
)

var ModeMap = map[string]map[string]string{
    "trade": {"trades": "1"},
    "L2":    {"l2_orderbook": "4"},
}

var httpClient = &http.Client{Timeout: 120 * time.Second}

// ─────────────────────────────────────────
// Struct
// ─────────────────────────────────────────

type DownloadLink struct {
    Filename string
    URL      string
    SizeMB   interface{}
}

type APIResponse struct {
    Code string `json:"code"`
    Data struct {
        Details []struct {
            GroupDetails []struct {
                Filename string      `json:"filename"`
                URL      string      `json:"url"`
                SizeMB   interface{} `json:"sizeMB"`
            } `json:"groupDetails"`
        } `json:"details"`
    } `json:"data"`
}

// L2 orderbook row (parquet schema)
type OrderbookRow struct {
    Ts      int64   `parquet:"ts"`
    Action  string  `parquet:"action"`
    Side    string  `parquet:"side"`
    Px      float32 `parquet:"px"`
    Sz      float32 `parquet:"sz"`
    Orders  int32   `parquet:"orders"`
}

// Generic CSV row stored as map → converted to parquet via dynamic schema
// We'll write CSV data as raw parquet rows using map[string]interface{}

// ─────────────────────────────────────────
// HTTP helpers
// ─────────────────────────────────────────

func doPostWithRetry(payload interface{}) (*APIResponse, error) {
    body, err := json.Marshal(payload)
    if err != nil {
        return nil, err
    }
    ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
    url := API + "?t=" + ts

    for attempt := 1; attempt <= MaxRetry; attempt++ {
        req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
        req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Referer", "https://www.okx.com/historical-data")

        resp, err := httpClient.Do(req)
        if err != nil {
            if attempt < MaxRetry {
                time.Sleep(RetryDelay * time.Duration(attempt))
            }
            continue
        }
        defer resp.Body.Close()

        var apiResp APIResponse
        if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
            if attempt < MaxRetry {
                time.Sleep(RetryDelay * time.Duration(attempt))
            }
            continue
        }
        return &apiResp, nil
    }
    return nil, fmt.Errorf("all retries exhausted")
}

func fetchDownloadLinks(date time.Time, module, instType, instFamily string) ([]DownloadLink, error) {
    start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
    end := start.Add(24*time.Hour - time.Millisecond)

    payload := map[string]interface{}{
        "module":   module,
        "instType": instType,
        "instQueryParam": map[string]interface{}{
            "instFamilyList": []string{instFamily},
        },
        "dateQuery": map[string]interface{}{
            "dateAggrType": "daily",
            "begin":        strconv.FormatInt(start.UnixMilli(), 10),
            "end":          strconv.FormatInt(end.UnixMilli(), 10),
        },
    }

    apiResp, err := doPostWithRetry(payload)
    if err != nil || apiResp.Code != "0" {
        return nil, err
    }

    var results []DownloadLink
    for _, detail := range apiResp.Data.Details {
        for _, g := range detail.GroupDetails {
            results = append(results, DownloadLink{
                Filename: g.Filename,
                URL:      g.URL,
                SizeMB:   g.SizeMB,
            })
        }
    }
    return results, nil
}

func downloadFile(url, localPath string) bool {
    for attempt := 1; attempt <= MaxRetry; attempt++ {
        req, _ := http.NewRequest("GET", url, nil)
        req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        req.Header.Set("Referer", "https://www.okx.com/historical-data")

        resp, err := httpClient.Do(req)
        if err != nil {
            if attempt < MaxRetry {
                time.Sleep(RetryDelay * time.Duration(attempt))
            }
            continue
        }
        if resp.StatusCode == 404 {
            resp.Body.Close()
            return false
        }
        if resp.StatusCode != 200 {
            resp.Body.Close()
            if attempt < MaxRetry {
                time.Sleep(RetryDelay * time.Duration(attempt))
            }
            continue
        }

        f, err := os.Create(localPath)
        if err != nil {
            resp.Body.Close()
            return false
        }
        _, copyErr := io.Copy(f, resp.Body)
        f.Close()
        resp.Body.Close()

        if copyErr != nil {
            os.Remove(localPath)
            if attempt < MaxRetry {
                time.Sleep(RetryDelay * time.Duration(attempt))
            }
            continue
        }

        info, _ := os.Stat(localPath)
        if info == nil || info.Size() == 0 {
            os.Remove(localPath)
            return false
        }
        return true
    }
    return false
}

// ─────────────────────────────────────────
// Parquet write in
// ─────────────────────────────────────────

func writeOrderbookRowsToParquet(rows []OrderbookRow, path string) (int, error) {
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    w := parquet.NewGenericWriter[OrderbookRow](f, parquet.Snappy)
    n, err := w.Write(rows)
    if err != nil {
        return n, err
    }
    return n, w.Close()
}

// CSV → Parquet: we use dynamic schema via map rows
type CSVRow map[string]string

func writeCSVToParquet(records [][]string, headers []string, path string) (int, error) {
    // Build schema dynamically from headers
    // Use string columns for all fields (simplest safe approach)
    schema := parquet.SchemaOf(new(struct{}))
    _ = schema

    // Use raw parquet writer with string schema
    type DynRow = map[string]interface{}

    rows := make([]DynRow, 0, len(records))
    for _, rec := range records {
        row := make(DynRow, len(headers))
        for i, h := range headers {
            if i < len(rec) {
                row[h] = rec[i]
            } else {
                row[h] = ""
            }
        }
        rows = append(rows, row)
    }

    // Write using parquet-go generic writer with map schema
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    w := parquet.NewGenericWriter[DynRow](f, parquet.Snappy)
    n, err := w.Write(rows)
    if err != nil {
        return n, err
    }
    return n, w.Close()
}

// ─────────────────────────────────────────
// Orderbook NDJSON parser
// ─────────────────────────────────────────

func parseOrderbookNDJSON(r io.Reader, parquetPath string) (int, error) {
    type RawLevel [3]interface{}
    type RawEntry struct {
        Ts     interface{}   `json:"ts"`
        Action string        `json:"action"`
        Asks   []interface{} `json:"asks"`
        Bids   []interface{} `json:"bids"`
    }

    decoder := json.NewDecoder(r)
    var buf []OrderbookRow
    total := 0
    const batchSize = 100_000

    flush := func() error {
        if len(buf) == 0 {
            return nil
        }
        n, err := writeOrderbookRowsToParquet(buf, parquetPath)
        total += n
        buf = buf[:0]
        return err
    }

    for decoder.More() {
        var entry RawEntry
        if err := decoder.Decode(&entry); err != nil {
            continue
        }

        var ts int64
        switch v := entry.Ts.(type) {
        case float64:
            ts = int64(v)
        case string:
            ts, _ = strconv.ParseInt(v, 10, 64)
        }

        parseLevel := func(side string, levels []interface{}) {
            for _, lv := range levels {
                arr, ok := lv.([]interface{})
                if !ok || len(arr) < 2 {
                    continue
                }
                px := toFloat32(arr[0])
                sz := toFloat32(arr[1])
                orders := int32(0)
                if len(arr) > 2 {
                    orders = int32(toFloat64(arr[2]))
                }
                buf = append(buf, OrderbookRow{
                    Ts:     ts,
                    Action: entry.Action,
                    Side:   side,
                    Px:     px,
                    Sz:     sz,
                    Orders: orders,
                })
            }
        }

        parseLevel("asks", entry.Asks)
        parseLevel("bids", entry.Bids)

        if len(buf) >= batchSize {
            if err := flush(); err != nil {
                return total, err
            }
        }
    }
    flush()
    return total, nil
}

func toFloat32(v interface{}) float32 {
    switch x := v.(type) {
    case float64:
        return float32(x)
    case string:
        f, _ := strconv.ParseFloat(x, 32)
        return float32(f)
    }
    return 0
}

func toFloat64(v interface{}) float64 {
    switch x := v.(type) {
    case float64:
        return x
    case string:
        f, _ := strconv.ParseFloat(x, 64)
        return f
    }
    return 0
}

// ─────────────────────────────────────────
// CSV parser (chunked)
// ─────────────────────────────────────────

func parseCSVToParquet(r io.Reader, parquetPath string) (int, error) {
    reader := csv.NewReader(r)
    reader.LazyQuotes = true
    reader.TrimLeadingSpace = true

    headers, err := reader.Read()
    if err != nil {
        return 0, err
    }

    const chunkSize = 300_000
    var chunk [][]string
    total := 0

    flush := func() error {
        if len(chunk) == 0 {
            return nil
        }
        n, err := writeCSVToParquet(chunk, headers, parquetPath)
        total += n
        chunk = chunk[:0]
        return err
    }

    for {
        rec, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            continue
        }
        chunk = append(chunk, rec)
        if len(chunk) >= chunkSize {
            if err := flush(); err != nil {
                return total, err
            }
        }
    }
    flush()
    return total, nil
}

// ─────────────────────────────────────────
// unzip
// ─────────────────────────────────────────

func isNDJSON(data []byte) bool {
    trimmed := strings.TrimSpace(string(data))
    return strings.HasPrefix(trimmed, "{")
}

func extractAndConvert(rawPath, parquetPath string) bool {
    totalRows := 0

    switch {
    case strings.HasSuffix(rawPath, ".zip"):
        zr, err := zip.OpenReader(rawPath)
        if err != nil {
            log.Printf("zip open error: %v", err)
            return false
        }
        defer zr.Close()

        for _, f := range zr.File {
            if strings.HasSuffix(f.Name, "/") {
                continue
            }
            rc, err := f.Open()
            if err != nil {
                continue
            }
            n, err := parseCSVToParquet(rc, parquetPath)
            rc.Close()
            if err == nil {
                totalRows += n
            }
        }

    case strings.HasSuffix(rawPath, ".tar.gz"):
        f, err := os.Open(rawPath)
        if err != nil {
            return false
        }
        defer f.Close()

        gr, err := gzip.NewReader(f)
        if err != nil {
            return false
        }
        defer gr.Close()

        tr := tar.NewReader(gr)
        for {
            hdr, err := tr.Next()
            if err == io.EOF {
                break
            }
            if err != nil || hdr.Typeflag != tar.TypeReg {
                continue
            }

            // Peek first 10 bytes to determine format
            peek := make([]byte, 10)
            n, _ := io.ReadFull(tr, peek)
            combined := io.MultiReader(bytes.NewReader(peek[:n]), tr)

            var rows int
            if isNDJSON(peek[:n]) {
                rows, err = parseOrderbookNDJSON(combined, parquetPath)
            } else {
                rows, err = parseCSVToParquet(combined, parquetPath)
            }
            if err == nil {
                totalRows += rows
            }
        }

    case strings.HasSuffix(rawPath, ".csv.gz"):
        f, err := os.Open(rawPath)
        if err != nil {
            return false
        }
        defer f.Close()

        gr, err := gzip.NewReader(f)
        if err != nil {
            return false
        }
        defer gr.Close()

        n, err := parseCSVToParquet(gr, parquetPath)
        if err == nil {
            totalRows += n
        }

    default:
        log.Printf("unsupported format: %s", rawPath)
        return false
    }

    return totalRows > 0
}

// ─────────────────────────────────────────
// main
// ─────────────────────────────────────────

type Task struct {
    Date   time.Time
    MName  string
    MID    string
}

func runSync(mode, startDateStr string, daysBack int, symbol string) {
    // 解析 symbol
    parts := strings.Split(symbol, "-")
    var instType, instFamily string
    if len(parts) >= 3 {
        instType = strings.ToUpper(parts[len(parts)-1])
        instFamily = strings.ToUpper(strings.Join(parts[:len(parts)-1], "-"))
    } else {
        instType = "SPOT"
        instFamily = strings.ToUpper(symbol)
    }

    // 解析日期
    baseDate, err := time.ParseInLocation("2006-01-02", startDateStr, time.UTC)
    if err != nil {
        log.Fatalf("Date format unavailable，please use:  YYYY-MM-DD: %v", err)
    }

    // 建立日期列表
    dates := make([]time.Time, daysBack)
    for i := 0; i < daysBack; i++ {
        dates[i] = baseDate.AddDate(0, 0, -i)
    }
    sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

    // 決定下載模組
    activeModules := map[string]string{}
    if mode == "all" {
        for k, v := range ModeMap["trade"] {
            activeModules[k] = v
        }
        for k, v := range ModeMap["L2"] {
            activeModules[k] = v
        }
    } else {
        for k, v := range ModeMap[mode] {
            activeModules[k] = v
        }
    }

    log.Printf("Target: %s (%s) mode: %s", instFamily, instType, mode)
    log.Printf("Date range: %s → %s", dates[0].Format("2006-01-02"), dates[len(dates)-1].Format("2006-01-02"))

    // 建立任務列表
    var tasks []Task
    for _, d := range dates {
        for mName, mID := range activeModules {
            tasks = append(tasks, Task{Date: d, MName: mName, MID: mID})
        }
    }

    var (
        mu           sync.Mutex
        successCount int
        failCount    int
        skipCount    int
    )

    bar := progressbar.NewOptions(len(tasks),
        progressbar.OptionSetDescription("Total"),
        progressbar.OptionShowCount(),
        progressbar.OptionSetTheme(progressbar.Theme{
            Saucer:        "=",
            SaucerHead:    ">",
            SaucerPadding: " ",
            BarStart:      "[",
            BarEnd:        "]",
        }),
    )

    for _, task := range tasks {
        dStr := task.Date.Format("2006-01-02")
        bar.Describe(fmt.Sprintf("[%s/%s]", dStr, task.MName))

        files, err := fetchDownloadLinks(task.Date, task.MID, instType, instFamily)
        if err != nil || len(files) == 0 {
            mu.Lock()
            failCount++
            mu.Unlock()
            bar.Add(1)
            continue
        }

        for _, f := range files {
            safeName := strings.ReplaceAll(f.Filename, " ", "_")
            finalParquet := filepath.Join(ParquetDir, safeName+".parquet")
            tmpRaw := filepath.Join(RawDir, safeName)

            if _, err := os.Stat(finalParquet); err == nil {
                mu.Lock()
                skipCount++
                mu.Unlock()
                continue
            }

            if downloadFile(f.URL, tmpRaw) {
                if extractAndConvert(tmpRaw, finalParquet) {
                    os.Remove(tmpRaw)
                    mu.Lock()
                    successCount++
                    mu.Unlock()
                } else {
                    mu.Lock()
                    failCount++
                    mu.Unlock()
                }
            } else {
                mu.Lock()
                failCount++
                mu.Unlock()
            }
        }
        bar.Add(1)
    }

    fmt.Printf("\nComplete: Success %d, Skip %d, Fail %d\n", successCount, skipCount, failCount)
}

func main() {
    os.MkdirAll(RawDir, 0755)
    os.MkdirAll(ParquetDir, 0755)

    mode := flag.String("mode", "all", "download mode: trade, L2, 或 all (預設)")
    intervalDate := flag.String("date", time.Now().Format("2006-01-02"), "start date YYYY-MM-DD")
    intervalDays := flag.Int("days", 7, "look back")
    symbol := flag.String("symbol", "ETH-USDT-SWAP", "Symbols, etc. ETH-USDT-SWAP or BTC-USDT")

    flag.Parse()

    runSync(*mode, *intervalDate, *intervalDays, *symbol)
}