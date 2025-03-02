package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"strings"

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

type PivotPoints struct {
    Pivot float64
    R1    float64
    R2    float64
    R3    float64
    S1    float64
    S2    float64
    S3    float64
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

	// Variables for Fibonacci Pivot Points
	var high, low, closePrice float64

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
		// fmt.Printf("Read %d rows from %s\n", n, filePath)

		// Process each row
		for i := 0; i < n; i++ {
			row := rows[i]
			if row.Date == 0 { // Skip rows with invalid timestamps
				continue
			}
			timestamp := time.Unix(0, row.Date).UTC() // Convert int64 (nanoseconds) to time.Time

			// Debug: Log the timestamp and row data
			// fmt.Printf("Processing row: Date=%v, Open=%v, High=%v, Low=%v, Close=%v, Volume=%v\n",
			// 	timestamp, row.Open, row.High, row.Low, row.Close, row.Volume)
			
			// Filter for 10th Jan 2024
			if timestamp.Format("2006-01-02") != "2024-01-10" {
				continue
			}

			// Update high, low, and close for the day
			if row.High > high {
				high = row.High
			}
			if row.Low < low || low == 0 {
				low = row.Low
			}
			closePrice = row.Close

			// Generate 5-minute interval key
			intervalKey := timestamp.Truncate(5 * time.Minute).Format("2006-01-02 15:04")

			// Update candle data
			if _, exists := candles[intervalKey]; !exists {
				candles[intervalKey] = &Candle{Open: row.Open, High: row.High, Low: row.Low, Close: row.Close}
			} else {
				// Update existing candle
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
	// Calculate Fibonacci Pivot Points
	pivotPoints := calculatePivotPoints(high, low, closePrice)

	// // Display Pivot Points
	// fmt.Println("Fibonacci Pivot Points for 2024-01-10:")
	// fmt.Printf("Pivot: %.2f\n", pivotPoints.Pivot)
	// fmt.Printf("R1: %.2f, R2: %.2f, R3: %.2f\n", pivotPoints.R1, pivotPoints.R2, pivotPoints.R3)
	// fmt.Printf("S1: %.2f, S2: %.2f, S3: %.2f\n", pivotPoints.S1, pivotPoints.S2, pivotPoints.S3)

	// Save to CSV
	saveToCSV(candles, outputFolder, filepath.Base(filePath))

	// Save Pivot Points to a new CSV file
	savePivotPointsToCSV(pivotPoints, outputFolder, filepath.Base(filePath))
}

func calculatePivotPoints(high, low, close float64) PivotPoints {
    pivot := (high + low + close) / 3
    return PivotPoints{
        Pivot: pivot,
        R1:    pivot + 0.382*(high-low),
        R2:    pivot + 0.618*(high-low),
        R3:    pivot + (high - low),
        S1:    pivot - 0.382*(high-low),
        S2:    pivot - 0.618*(high-low),
        S3:    pivot - (high - low),
    }
}

// Save aggregated candles to a CSV file
func saveToCSV(candles map[string]*Candle, outputFolder string, fileName string) {
	fileName = strings.TrimSuffix(fileName, ".parquet")
	outputPath := filepath.Join(outputFolder, fileName+".csv")
	
	file, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Interval", "Open", "High", "Low", "Close"}); err != nil {
		log.Fatalf("Failed to write header to CSV: %v", err)
	}

	// Write candle data
	for interval, candle := range candles {
		record := []string{
			interval,
			fmt.Sprintf("%.2f", candle.Open),
			fmt.Sprintf("%.2f", candle.High),
			fmt.Sprintf("%.2f", candle.Low),
			fmt.Sprintf("%.2f", candle.Close),
		}
		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write record to CSV: %v", err)
		}
	}
}

// Save Pivot Points to a new CSV file
func savePivotPointsToCSV(pivotPoints PivotPoints, outputFolder string, fileName string) {
	fileName = strings.TrimSuffix(fileName, ".parquet")
	outputPath := filepath.Join(outputFolder, "pivot_points_"+fileName+".csv")
	file, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Pivot", "R1", "R2", "R3", "S1", "S2", "S3"}); err != nil {
		log.Fatalf("Failed to write header to CSV: %v", err)
	}

	// Write pivot points data
	record := []string{
		fmt.Sprintf("%.2f", pivotPoints.Pivot),
		fmt.Sprintf("%.2f", pivotPoints.R1),
		fmt.Sprintf("%.2f", pivotPoints.R2),
		fmt.Sprintf("%.2f", pivotPoints.R3),
		fmt.Sprintf("%.2f", pivotPoints.S1),
		fmt.Sprintf("%.2f", pivotPoints.S2),
		fmt.Sprintf("%.2f", pivotPoints.S3),
	}
	if err := writer.Write(record); err != nil {
		log.Fatalf("Failed to write record to CSV: %v", err)
	}
}

func main() {
	inputFolder := "data"          // Folder containing Parquet files
	outputFolder := "5min_candles" // Folder to save CSV files
	processParquetFiles(inputFolder, outputFolder)
}
