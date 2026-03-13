# Multi-Source ETL Pipeline with Python

## Overview

This project implements a **simple ETL (Extract, Transform, Load) pipeline** using Python and Pandas.

The pipeline extracts data from **multiple file formats**:

* CSV
* JSON
* XML

All data sources are processed, transformed, and consolidated into a **single structured dataset**.

This project is part of my **Data Engineering Monorepo**, where I implement small pipelines to practice core data engineering concepts.

---

## Architecture

Multi-Format Sources → Extraction → Transformation → Load → Output Dataset

```
CSV Files
JSON Files
XML Files
     │
     ▼
   Extract
     │
     ▼
  Transform
     │
     ▼
    Load
     │
     ▼
transformed_data.csv
```

---

## Technologies Used

* Python
* Pandas
* XML Parsing (`xml.etree.ElementTree`)
* File Pattern Processing (`glob`)
* Logging
* CSV / JSON / XML ingestion

---

## Project Structure

```
multi-source-etl/

│
├── etl_pipeline.py
├── source1.csv
├── source2.json
├── source3.xml
├── transformed_data.csv
├── log_file.txt
└── README.md
```

---

## Data Sources

The pipeline ingests structured datasets from different formats.

### CSV Example

| name  | height | weight |
| ----- | ------ | ------ |
| alex  | 65.78  | 112.99 |
| ajay  | 71.52  | 136.49 |
| alice | 69.40  | 153.03 |

---

### JSON Example

```
{"name":"alex","height":65.78,"weight":112.99}
{"name":"ajay","height":71.52,"weight":136.49}
```

---

### XML Example

```xml
<data>
  <person>
    <name>simon</name>
    <height>67.90</height>
    <weight>112.37</weight>
  </person>
</data>
```

---

## Pipeline Steps

### 1️⃣ Extract

The pipeline scans the working directory and loads all supported files.

Supported formats:

* `.csv`
* `.json`
* `.xml`

Example:

```python
for csvfile in glob.glob("*.csv"):
```

Each format has its own extraction function.

---

### 2️⃣ Transform

Basic transformation is applied to standardize numeric values.

Example:

```python
data["price"] = round(data.price, 2)
```

This step simulates **data normalization and preparation for analytics**.

---

### 3️⃣ Load

The final dataset is saved as a consolidated CSV file.

```python
transformed_data.to_csv(target_file)
```

Output:

```
transformed_data.csv
```

---

## Logging

The pipeline logs each execution phase:

* ETL Job Started
* Extract phase
* Transform phase
* Load phase
* ETL Job Finished

Example log entry:

```
2026-Feb-10 21:14:02, Extract phase Started
```

---

## Example Output

After processing all sources, the pipeline produces a unified dataset:

| name  | height | weight |
| ----- | ------ | ------ |
| alex  | 65.78  | 112.99 |
| simon | 67.90  | 112.37 |
| cindy | 66.49  | 127.45 |

---

## Learning Objectives

This project demonstrates important **data engineering skills**:

* Multi-source data ingestion
* Handling heterogeneous data formats
* Building simple ETL pipelines
* Logging ETL execution
* Data transformation using Pandas
* Consolidating datasets for downstream analytics

---

## Possible Improvements

Future enhancements could include:

* Schema validation
* Data quality checks
* Dockerizing the pipeline
* Scheduling execution with Airflow
* Writing output to a data warehouse

---

## Author

**Paulo Potter Marchi**

Data Analyst transitioning into **Data Engineering**

Core skills:

* SQL
* Data Modeling
* ETL / ELT
* Python
* Data Warehousing
* Power BI

