# Largest Banks ETL Pipeline

## Overview

This project implements a **complete ETL (Extract, Transform, Load) pipeline** that collects financial data about the world's largest banks by market capitalization.

The pipeline extracts the data from an archived Wikipedia page, transforms the dataset by converting market capitalization into multiple currencies, and loads the processed data into both a **CSV file** and a **SQLite database**.

This project demonstrates fundamental **data engineering concepts**, including web scraping, data transformation, relational storage, and pipeline logging.

---

## Architecture

```
Archived Web Page
        │
        ▼
   Extract (BeautifulSoup)
        │
        ▼
 Transform (Currency Conversion)
        │
        ▼
 Load
 ├── CSV File
 └── SQLite Database
```

---

## Technologies Used

* Python
* Pandas
* NumPy
* BeautifulSoup
* Requests
* SQLite
* Logging

---

## Data Source

The data is extracted from an archived version of the Wikipedia page:

List of Largest Banks by Market Capitalization.

The pipeline collects the following attributes:

| Column         | Description                             |
| -------------- | --------------------------------------- |
| Name           | Bank name                               |
| MC_USD_Billion | Market capitalization in USD (billions) |

---

## Transformation

The pipeline converts the market capitalization values into multiple currencies using predefined exchange rates.

Additional columns created:

| Column         | Description                             |
| -------------- | --------------------------------------- |
| MC_GBP_Billion | Market capitalization in British Pounds |
| MC_EUR_Billion | Market capitalization in Euros          |
| MC_INR_Billion | Market capitalization in Indian Rupees  |

Exchange rates are provided in the file:

```
exchange_rate.csv
```

Example:

```
Currency,Rate
EUR,0.93
GBP,0.8
INR,82.95
```

---

## Outputs

After processing, the pipeline generates two outputs.

### CSV Dataset

```
Largest_banks_data.csv
```

### SQLite Database

```
Banks.db
```

Database table created:

```
Largest_banks
```

---

## Example Queries

The pipeline runs SQL queries to validate the stored data.

Retrieve all records:

```sql
SELECT * FROM Largest_banks
```

Average market capitalization in GBP:

```sql
SELECT AVG(MC_GBP_Billion) FROM Largest_banks
```

Retrieve the first five banks:

```sql
SELECT Name FROM Largest_banks LIMIT 5
```

---

## Logging

The ETL execution progress is recorded in:

```
code_log.txt
```

Example log entry:

```
2026-Mar-13-21:42:11, Data extraction complete
```

---

## Project Structure

```
largest-banks-etl-pipeline
│
├── banks_etl_pipeline.py
├── exchange_rate.csv
├── Largest_banks_data.csv
├── Banks.db
├── code_log.txt
└── README.md
```

---

## Learning Objectives

This project demonstrates key **data engineering skills**:

* Web scraping structured data
* Designing ETL pipelines
* Data transformation with Pandas
* Currency conversion logic
* Loading data into relational databases
* Executing SQL queries
* Logging pipeline execution

---

## Author

**Paulo Potter Marchi**

Data Analyst transitioning into **Data Engineering**
