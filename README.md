# Data Engineering Monorepo

Hello, World! 👋

This repository is a **monorepo containing multiple data engineering projects**, created to practice and demonstrate real-world data workflows and engineering concepts.

The projects included here explore the **full lifecycle of data pipelines**, from raw data ingestion to transformation, storage, and analytics-ready datasets.

---

## Technologies & Concepts

This repository explores several core areas of **Data Engineering**:

* Python for data pipelines
* SQL for relational data modeling
* Pandas for data transformation
* ETL / ELT pipeline development
* Data ingestion from multiple formats (CSV, JSON, XML)
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

## Repository Structure

```text
data-engineering-monorepo
│
├── SQLite_Pandas
│   ├── db_code.py
│   ├── INSTRUCTOR.csv
│   ├── Departments.csv
│   └── README.md
│
├── etl
│   ├── etl_practice.py
│   ├── source.zip
│   └── README.md
│
├── site-exemplo
│
└── README.md
```

---

## Projects

### SQLite + Pandas Data Ingestion

Demonstrates loading CSV files into a SQLite database using Pandas and executing SQL queries.

Concepts explored:

* Data ingestion
* SQL queries from Python
* Table creation
* Data appending

---

### Multi-Source ETL Pipeline

Implements a simple ETL pipeline capable of extracting data from:

* CSV
* JSON
* XML

The pipeline consolidates the data into a unified dataset and logs the execution stages.

Concepts explored:

* Multi-source ingestion
* Data transformation
* ETL logging
* Data consolidation

---

## Purpose

The purpose of this repository is to:

* Practice **data engineering fundamentals**
* Build a **portfolio of practical projects**
* Explore **different pipeline architectures**
* Document my learning journey into Data Engineering

---

## Author

**Paulo Potter Marchi**

Data Analyst transitioning into **Data Engineering**

Skills:

* SQL
* Data Modeling
* ETL / ELT
* Python
* Data Warehousing
* Power BI
