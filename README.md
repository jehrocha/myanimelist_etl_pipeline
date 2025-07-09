# MyAnimeList ETL Pipeline
A basic data engineering project fetching data from Jikan API and creating a snowflake schema loaded into a postgres server


## Setup

1. Clone the repository
```
git clone https://github.com/jehrocha/myanimelist_etl_pipeline
```
2. Create a virtual environment
```
python3 -m venv airflow_venv
source airflow_venv/bin/activate
```
3. Install dependencies
```
bash setup.sh
```
4. Install Docker
5. 
See https://www.docker.com/products/docker-desktop/

Run the following commands
```
docker compose up airflow-init
docker compose up -d
```
To stop the airflow container, run:
```
docker compose down
```
