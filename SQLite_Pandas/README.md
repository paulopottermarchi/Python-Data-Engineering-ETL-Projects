# SQLite Data Ingestion Pipeline with Python

## Overview

This project demonstrates a simple **data ingestion and querying pipeline** using **Python, Pandas, and SQLite**.

The goal of this project is to simulate a **basic ETL/ELT workflow**, where CSV files are ingested into a relational database and then queried using SQL.

This project is part of my **Data Engineering Monorepo**, which contains multiple small projects designed to practice core data engineering concepts such as:

* Data ingestion
* Data transformation
* Relational database loading
* SQL querying
* Lightweight local data pipelines

---

## Architecture

CSV Files → Pandas → SQLite Database → SQL Queries → Data Output

The pipeline performs the following operations:

1. Reads structured CSV files
2. Loads them into a SQLite relational database
3. Executes SQL queries
4. Appends new data into tables
5. Retrieves analytical results

---

## Technologies Used

* Python
* Pandas
* SQLite
* SQL

---

## Project Structure

```
sqlite-data-ingestion/

│
├── main.py
├── INSTRUCTOR.csv
├── Departments.csv
└── STAFF.db
```

---

## Dataset Description

### Instructor Table

| Column | Description   |
| ------ | ------------- |
| ID     | Instructor ID |
| FNAME  | First Name    |
| LNAME  | Last Name     |
| CITY   | City          |
| CCODE  | Country Code  |

Example:

```
1,Rav,Ahuja,TORONTO,CA
2,Raul,Chong,Markham,CA
3,Hima,Vasudevan,Chicago,US
```

---

### Departments Table

| Column     | Description     |
| ---------- | --------------- |
| DEPT_ID    | Department ID   |
| DEP_NAME   | Department Name |
| MANAGER_ID | Manager ID      |
| LOC_ID     | Location ID     |

Example:

```
2,Architect Group,30001,L0001
5,Software Group,30002,L0002
7,Design Team,30003,L0003
```

---

## Pipeline Steps

### 1️⃣ Connect to SQLite Database

```python
conn = sqlite3.connect("STAFF.db")
```

Creates or connects to a SQLite database.

---

### 2️⃣ Load CSV Data

```python
df = pd.read_csv(file_path, names=attribute_list)
```

Reads structured CSV files into a Pandas DataFrame.

---

### 3️⃣ Load Data into Database

```python
df.to_sql(table_name, conn, if_exists="replace", index=False)
```

Writes the DataFrame into a SQLite table.

---

### 4️⃣ Execute SQL Queries

Example queries executed in the project:

```sql
SELECT * FROM INSTRUCTOR
SELECT FNAME FROM INSTRUCTOR
SELECT COUNT(*) FROM INSTRUCTOR
```

---

### 5️⃣ Append New Data

```python
data_append.to_sql(table_name, conn, if_exists="append", index=False)
```

Adds new records to the existing table.

---

## Example Output

Example query:

```
SELECT COUNT(*) FROM INSTRUCTOR
```

Result:

```
14
```

After appending a new record:

```
15
```

---

## Learning Objectives

This project demonstrates practical skills used in data engineering:

* File-based data ingestion
* Working with relational databases
* Writing SQL queries from Python
* Table creation and data insertion
* Appending and validating records
* Managing lightweight pipelines

---

## Possible Improvements

Future enhancements could include:

* Using **SQLAlchemy for database abstraction**
* Creating **data validation checks**
* Implementing **logging**
* Turning the script into a **modular pipeline**
* Running the pipeline using **Airflow or Prefect**

---

## Author

**Paulo Potter Marchi**

Data Analyst transitioning to **Data Engineering**

Skills:

* SQL
* Data Modeling
* ETL / ELT
* Power BI
* Python
* Data Warehousing

