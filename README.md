<h1 align="center">
GitHub-Most-Popular-Repos
<h1 align="center">

## Overview 
This project builds a data pipeline to process GitHub's most popular repositories using Apache Spark and store the results in PostgreSQL. The pipeline extracts insights into programming languages, top organizations, and search term relevance based on GitHub repository data.

## Features  
- **Extracts JSON data** from multiple GitHub repository files.
- **Processes data** using Apache Spark.  
- **Stores results** in PostgreSQL.  
- **Uses Docker & Docker Compose** for easy deployment.  
- **Performs analysis** on programming languages, organizations, and search term relevance.  

## **Technologies Used**
- **Apache Spark** - For large-scale data processing
- **PostgreSQL** - To store data
- **Docker & Docker Compose** - For containerized deployment
- **PySpark** - For ETL Pipeline  
- **pgAdmin** - To visualize the stored data

## 🛠Setup Instructions  
### 1. Clone the Repository  
```bash
git clone https://github.com/nahlarmash/GitHub-Repos-Pipeline.git  
```

### 2. Run the Project using Docker
```bash
docker-compose up
```

This will:

- Start Apache Spark and PostgreSQL.
- Process the JSON files using `main.py`.
- Store the processed data in PostgreSQL.

## Data Processing & Results

### 1️- Programming Languages Analysis

We group repositories by their programming language and count how many repos exist for each language.

#### SQL Query
```
SELECT * FROM programming_lang order by repo_count DESC;
```

#### Results:
<img width="400" height="500" alt="image" src="https://github.com/nahlarmash/GitHub-Repos-Pipeline/blob/main/Screenshots/programing_lang.png">
</h1> 

### 2️- Organizations & Stars Analysis

We filter repositories owned by organizations and sum up their total stars.

#### SQL Query
```
SELECT * FROM organizations_stars ORDER BY total_stars DESC;
```

#### Results:
<img width="400" height="500" alt="image" src="https://github.com/nahlarmash/GitHub-Repos-Pipeline/blob/main/Screenshots/organizations_stars.png">
</h1> 


### 3️- Search Term Relevance

We calculate a relevance score for each search term using:

- Forks
- Subscribers
- Stars

#### Formula:
```
relevance_score = (1.5 * forks) + (1.32 * subscribers) + (1.04 * stars)
```

#### SQL Query
```
SELECT * FROM organizations_stars ORDER BY total_stars DESC;
```

#### Results:
<img width="400" height="500" alt="image" src="https://github.com/nahlarmash/GitHub-Repos-Pipeline/blob/main/Screenshots/search_terms_relevance.png">
</h1> 

## Accessing PostgreSQL & pgAdmin

- pgAdmin UI: http://localhost:5050
- PostgreSQL Connection Details:

  - Host: postgres
  - Port: 5432
  - User: admin
  - Password: password
