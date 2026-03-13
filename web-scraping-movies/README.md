
# Web Scraping Pipeline – Top 25 Movies (2000–2010)

## Overview

This project demonstrates a **web scraping pipeline that extracts movie ranking data from an archived webpage, filters the results, and stores the dataset in both CSV and SQLite formats.**

The pipeline retrieves a list of highly ranked films, applies filtering logic based on release year, and stores the top results for further analysis.

This project is part of a **Data Engineering practice monorepo**, showcasing different approaches to data ingestion and transformation.

---

## Architecture

Web Page → Scraping → Filtering → Storage

```text
Archived Web Page
       │
       ▼
Extract (BeautifulSoup)
       │
       ▼
Filter (Year between 2000–2010)
       │
       ▼
Load
 ├── CSV file
 └── SQLite database
```

---

## Technologies Used

* Python
* BeautifulSoup (HTML parsing)
* Requests (HTTP requests)
* Pandas (data processing)
* SQLite (data storage)

---

## Data Source

The dataset is scraped from the archived page:

**100 Most Highly Ranked Films**

The pipeline extracts film titles, release years, and Rotten Tomatoes ratings.

---

## Data Schema

| Column          | Description   |
| --------------- | ------------- |
| Film            | Movie title   |
| Year            | Release year  |
| Rotten Tomatoes | Critic rating |

---

## Data Filtering Logic

The script filters movies based on release year.

Only films released between:

```
2000 – 2010
```

are included in the final dataset.

---

## Example Output

| Film                  | Year | Rotten Tomatoes |
| --------------------- | ---- | --------------- |
| The Lord of the Rings | 2001 | 91%             |
| Gladiator             | 2000 | 77%             |
| The Dark Knight       | 2008 | 94%             |

---

## Output Files

The pipeline generates two outputs:

CSV dataset:

```
top_25_films.csv
```

SQLite database:

```
Movies25.db
```

Database table:

```
Top_25
```

---

## Project Structure

```
webscraping-movies
│
├── webscraping_movies.py
├── top_25_films.csv
├── Movies25.db
└── README.md
```

---

## Learning Objectives

This project demonstrates key **data engineering concepts**:

* Web scraping structured data from HTML
* Parsing tables using BeautifulSoup
* Applying filtering logic during ingestion
* Creating datasets with Pandas
* Storing processed data in SQLite
* Building small data pipelines

---

## Possible Improvements

Future improvements could include:

* Automated scheduling (Airflow / Cron)
* Data validation checks
* Handling pagination
* Creating analytical dashboards
* Loading data into a data warehouse

---

## Author

**Paulo Potter Marchi**

Data Analyst transitioning into **Data Engineering**
