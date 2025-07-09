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

See: https://www.docker.com/products/docker-desktop/

Run the following commands
```
docker compose up airflow-init
docker compose up -d
```
To stop the airflow container, run:
```
docker compose down
```

5. Create postgres connection in airflow

Get the IP Address of the postgres connection. In the terminal, run

```
docker ps
```

Copy the Container ID of the postgres image

![alt text](https://github.com/jehrocha/myanimelist_etl_pipeline/blob/main/images/docker%20ps.png?raw=true)

Run

```
docker inspect <Container ID>
```

Copy the IP Address given below and use this as the Host for the connection in airflow

![alt text](https://github.com/jehrocha/myanimelist_etl_pipeline/blob/main/images/network.png)

Go to http://localhost:8080/ and login with credentials:

username: airflow

password: airflow

In the UI, open the **Admin > Connections** page and click the + button to add a new connection.

Fill in the following details

- Connnection ID: anime_list_conn
- Connection Type: postgres
- Host: IP Address
- Login: airflow
- Password: 
- Port: 5432
- Database: anime

6. Create a postgres server in pgadmin

Go to http://localhost:5050/ and login with creadentials:

username: admin@admin.com

password: root

Click Add Server

Under **General > Name**, create a name for the server (e.g. anime_server)

Under **Connections**, fill in the following:

Hostname/address: (Host/IP Address from airflow connection)

Port: 5432

Maintenance database: postgres

Username: airflow

Password: airflow
