# Data Engineering Monorepo

Hello, World! 👋

This repository is a **monorepo containing multiple data engineering projects**, created to practice and demonstrate real-world data workflows and engineering concepts.

The projects included here explore the **full lifecycle of data pipelines**, from raw data ingestion to transformation, storage, and analytics-ready datasets.

---

# Data Pipeline Architecture

```text
Raw Data Sources
     │
     ▼
Data Ingestion
     │
     ▼
Data Transformation (ETL / ELT)
     │
     ▼
Data Storage
     │
     ▼
Analytics / Querying
```

This repository demonstrates different implementations of these stages using Python, SQL, and lightweight databases.

---

# Technologies & Concepts

This repository explores several core areas of **Data Engineering**:

* Python for data pipelines
* SQL for relational data modeling
* Pandas for data transformation
* ETL / ELT pipeline development
* Data ingestion from multiple formats (CSV, JSON, XML)
* Web scraping for data collection
* Relational databases (SQLite)
* Logging and pipeline monitoring
* Data warehouse concepts

Future expansions may include:

* Apache Airflow
* Apache Spark
* Cloud Data Warehouses
* Streaming pipelines
* Distributed data processing

---

# Repository Structure

```text
data-engineering-monorepo
│
├── sqlite-pandas-ingestion
│   ├── db_code.py
│   ├── INSTRUCTOR.csv
│   ├── Departments.csv
│   └── README.md
│
├── multi-source-etl
│   ├── etl_practice.py
│   ├── source.zip
│   └── README.md
│
├── largest-banks-etl-pipeline
│   ├── banks_etl_pipeline.py
│   ├── exchange_rate.csv
│   └── README.md
│
├── web-scraping-movies
│   ├── webscraping_movies.py
│   └── README.md
│
└── README.md
```

---

# Projects

## SQLite + Pandas Data Ingestion

Demonstrates loading CSV files into a SQLite database using Pandas and executing SQL queries.

Concepts explored:

* Data ingestion
* SQL queries from Python
* Table creation
* Data appending
* Local relational data storage

---

## Multi-Source ETL Pipeline

Implements an ETL pipeline capable of extracting data from multiple formats:

* CSV
* JSON
* XML

The pipeline consolidates the data into a unified dataset and logs the execution stages.

Concepts explored:

* Multi-source data ingestion
* Data transformation
* Pipeline logging
* Data consolidation

---

## Email-to-Bitrix Payment Automation

A real-world automation pipeline that monitors a corporate email inbox for 
payment report emails, extracts financial data from Excel attachments, and 
posts formatted summaries to a Bitrix24 group feed.

Pipeline steps:
1. Connect to corporate IMAP server and filter unread emails by subject
2. Extract and parse `.xlsx` attachment using openpyxl
3. Aggregate payment values and extract transaction date
4. Format and post message to Bitrix24 via REST API webhook
5. Mark email as read to prevent duplicate processing

Concepts explored:
- Email automation via IMAP
- Excel data extraction (openpyxl)
- REST API integration (Bitrix24)
- Environment variable management (.env / dotenv)
- Windows Task Scheduler automation
- Multi-client pipeline configuration

> This project was built and deployed in a **production environment** at MBA 
> Serviços de Cobranças, automating daily payment reporting for debt collection 
> portfolios (clients 501, 516).

---

## Largest Banks ETL Pipeline

A complete **web scraping ETL pipeline** that extracts market capitalization data for the world's largest banks from Wikipedia.

Pipeline steps:

1. Extract data via web scraping
2. Transform the dataset with currency conversions
3. Load the results into CSV and SQLite

Concepts explored:

* Web scraping
* Data transformation
* Currency conversion
* Database loading

---

## Movie Ranking Web Scraping Pipeline

Scrapes movie ranking data from an archived webpage, filters films based on release year, and stores the results in CSV and SQLite.

Concepts explored:

* HTML parsing with BeautifulSoup
* Data filtering logic
* Structured dataset creation
* Relational data storage

---

# Purpose

The purpose of this repository is to:

* Practice **data engineering fundamentals**
* Build a **portfolio of practical projects**
* Explore **different pipeline architectures**
* Document my learning journey into Data Engineering

---

# Author

**Paulo Potter Marchi**

Data Analyst transitioning into **Data Engineering**

Skills:

* SQL
* Data Modeling
* ETL / ELT
* Python
* Data Warehousing
* Power BI
