# MyAnimeList ETL Pipeline
A basic data engineering project fetching data from Jikan API and creating a snowflake schema loaded into a postgres server


## Setup

- Clone the repository
```
git clone https://github.com/jehrocha/myanimelist_etl_pipeline
```
- Create a virtual environment
```
python3 -m venv airflow_venv
source airflow_venv/bin/activate
```
- Install dependencies
```
bash setup.sh
```
