package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Data struct matching Parquet schema
type Data struct {
	Date   int64   `parquet:"date"`
	Open   float64 `parquet:"open"`
	High   float64 `parquet:"high"`
	Low    float64 `parquet:"low"`
	Close  float64 `parquet:"close"`
	Volume int64   `parquet:"volume"`
}

// Candle structure for 5-minute OHLC aggregation
type Candle struct {
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// Process all Parquet files in a folder
func processParquetFiles(inputFolder string, outputFolder string) {
	if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
		log.Fatalf("Failed to create output folder: %v", err)
	}

	// Process each Parquet file
	err := filepath.Walk(inputFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".parquet" {
			fmt.Println("Processing:", path)
			processParquetFile(path, outputFolder)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error reading files: %v", err)
	}
}

// Process a single Parquet file
func processParquetFile(filePath string, outputFolder string) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	// Create Parquet reader
	reader := parquet.NewGenericReader[Data](f)
	defer reader.Close()

	// Read data
	batchSize := 100 // Reduced batch size
	rows := make([]Data, batchSize)
	candles := make(map[string]*Candle)

	for {
		n, err := reader.Read(rows)
		if err != nil {
			if err.Error() == "EOF" {
				// End of file reached, break the loop
				fmt.Println("Reached end of file")
				break
			}
			log.Fatalf("Failed to read rows: %v", err)
		}
		if n == 0 {
			fmt.Println("No more rows to read")
			break
		}

		// Debug: Log the number of rows read
		fmt.Printf("Read %d rows from %s\n", n, filePath)

		// Process each row
		for i := 0; i < n; i++ {
			row := rows[i]
			if row.Date == 0 { // Skip rows with invalid timestamps
				continue
			}
			timestamp := time.Unix(0, row.Date).UTC() // Convert int64 (nanoseconds) to time.Time

			// Debug: Log the timestamp and row data
			fmt.Printf("Processing row: Date=%v, Open=%v, High=%v, Low=%v, Close=%v, Volume=%v\n",
				timestamp, row.Open, row.High, row.Low, row.Close, row.Volume)

			// Filter for 10th Jan 2024
			if timestamp.Format("2006-01-02") != "2024-01-10" {
				continue
			}

			// Generate 5-minute interval key
			intervalKey := timestamp.Truncate(5 * time.Minute).Format("2006-01-02 15:04")

			// Update candle data
			if _, exists := candles[intervalKey]; !exists {
				candles[intervalKey] = &Candle{Open: row.Open, High: row.High, Low: row.Low, Close: row.Close}
			} else {
				candle := candles[intervalKey]
				if row.High > candle.High {
					candle.High = row.High
				}
				if row.Low < candle.Low {
					candle.Low = row.Low
				}
				candle.Close = row.Close
			}
		}
	}
}


func main() {
	fmt.Println("Hello, World!")
}
