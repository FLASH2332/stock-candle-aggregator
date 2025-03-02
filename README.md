# **Stock Data Processor**

A Go-based tool to process stock market data stored in Parquet files. It aggregates data into **5-minute OHLC candles** and calculates **Fibonacci Pivot Points** for a specific date (10th Jan 2024). Results are saved as CSV files for further analysis.

---

## **Features**

- **5-Minute OHLC Candles**:
  - Aggregates stock data into 5-minute intervals.
  - Calculates **Open**, **High**, **Low**, and **Close** values for each interval.
  - Saves results to CSV files in the `5min_candles` folder.

- **Fibonacci Pivot Points**:
  - Calculates daily pivot points and support/resistance levels using Fibonacci formulas.
  - Displays results in the console and saves them to CSV files.

- **Batch Processing**:
  - Processes all Parquet files in the input folder (`data`).
  - Handles multiple files and saves results separately for each file.

---

## **Getting Started**

### **Prerequisites**
- Go installed on your system.
- Parquet files containing stock data in the required schema.

### **Installation**
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/stock-data-processor.git
   cd stock-data-processor
   ```

2. Install dependencies:
   ```bash
   go get github.com/parquet-go/parquet-go
   ```

3. Place your Parquet files in the `data` folder.

### **Usage**
Run the program:
```bash
go run main.go
```

- **Output**:
  - 5-minute candles: Saved in the `5min_candles` folder (e.g., `46900PE.csv`).
  - Pivot points: Saved in the `5min_candles` folder (e.g., `pivot_points_46900PE.csv`).

Debugging statements will be printed to your terminal if you uncomment them in the source-code

![WhatsApp Image 2025-03-02 at 15 02 26_70dd3735](https://github.com/user-attachments/assets/58ad81f2-97ef-4532-8746-e690e6ac2222)

---

## **Example Output**

### **5-Minute Candles CSV (`46900PE.csv`)**
```csvInterval,Open,High,Low,Close
2024-01-10 11:40,11.15,11.90,9.75,11.80
2024-01-10 11:45,11.80,13.65,11.00,12.05
2024-01-10 12:00,12.25,14.85,11.30,14.70
2024-01-10 12:50,19.45,24.85,16.70,17.75
2024-01-10 10:40,16.15,22.70,15.85,21.60
2024-01-10 13:00,18.95,20.95,16.30,18.10
2024-01-10 10:05,25.40,27.70,21.20,23.40
2024-01-10 12:30,14.50,16.25,11.45,11.75
...
```

### **Pivot Points CSV (`pivot_points_46900PE.csv`)**
```csv
Pivot,R1,R2,R3,S1,S2,S3
36.60,66.03,84.22,113.65,7.17,-11.02,-40.45
```

---

## **Challenges Faced**

1. **Data Download**:
   - Used `s3cmd` to download data from Cloudflare R2. Installation and authentication issues were resolved after multiple attempts.

2. **Reading Parquet Files**:
   - Initially used the outdated `gp-parquet` library. Switched to `parquet-go` for better support.
   - Debugged schema mismatches using the `parq` tool.

3. **Negative Support Levels**:
   - Some support levels had negative values, which were mathematically valid but unexpected.

---

## **Future Improvements**

- Support for multiple timeframes (e.g., 1-minute, 15-minute candles).
- Optimize performance for larger datasets.

---
