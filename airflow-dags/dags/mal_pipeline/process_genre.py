from datetime import timedelta
import pendulum

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Manila"),   
    schedule = None,
    catchup=False,
    tags=["genre","theme","demographic"],
    dagrun_timeout=timedelta(minutes=60),
)
def ProcessAnimeClassification():
    create_genre_table = SQLExecuteQueryOperator(
        task_id="create_genre_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS genre(
                genre_id SERIAL PRIMARY KEY,
                genre VARCHAR(15) UNIQUE
            );""",
        )
    
    create_theme_table = SQLExecuteQueryOperator(
        task_id="create_theme_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS theme(
                theme_id SERIAL PRIMARY KEY,
                theme VARCHAR(20) UNIQUE
            );""",
    )

    create_demographic_table = SQLExecuteQueryOperator(
        task_id="create_demographic_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS demographic(
                demographic_id SERIAL PRIMARY KEY,
                demographic VARCHAR(7) UNIQUE
        );""",
    )
    
    @task
    def extract_genre():
        genre_list = []
        url = "https://api.jikan.moe/v4/genres/anime"
        filters = ["genres", "explicit_genres"]

        for filter in filters:
            response = requests.get(url, params={"filter": filter})

            if response.status_code == 200:
                data = response.json()
                genres = data.get("data")

                for genre in genres:
                    name = genre.get("name")
                    genre_list.append((name,))

            else:
                print("Failed to retrieve the page")
                break
        
        genre_list.sort()
        return genre_list

    @task
    def load_genre(genre_list):
        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            query = """
                INSERT INTO genre (genre)
                VALUES (%s)
                ON CONFLICT (genre) DO NOTHING;
            """

            cur.executemany(query, genre_list)
            conn.commit()
        except Exception as e:
            print(f"An error occured: {e}")
            raise

    @task
    def extract_theme():
        theme_list = []
        url = "https://api.jikan.moe/v4/genres/anime"

        response = requests.get(url, params={"filter": "themes"})

        if response.status_code == 200:

            data = response.json()
            themes = data.get("data")

            for theme in themes:
                name = theme.get("name")
                theme_list.append((name,))
        
            print("Successfully extracted all themes")

        return theme_list
    
    @task()
    def load_theme(theme_list):
        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            query = """
                INSERT INTO theme (theme)
                VALUES (%s)
                ON CONFLICT (theme) DO NOTHING;
            """

            cur.executemany(query, theme_list)
            conn.commit()

        except Exception as e:
            print(f"An error occured: {e}")
            raise

    @task
    def extract_demographic():
        demographic_list = []
        url = "https://api.jikan.moe/v4/genres/anime"

        response = requests.get(url, params={"filter": "demographics"})

        if response.status_code == 200:

            data = response.json()
            genre = data.get("data")

            for genre in genre:
                name = genre.get("name")
                demographic_list.append((name,))
        
            print("Successfully extracted all demographics")

        return demographic_list
    
    @task
    def load_demographic(demographic_list):
        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            query = """
                INSERT INTO demographic (demographic)
                VALUES (%s)
                ON CONFLICT (demographic) DO NOTHING;
            """

            cur.executemany(query, demographic_list)
            conn.commit()

        except Exception as e:
            print(f"An error occured: {e}")
            raise

    genre_data = extract_genre()
    theme_data = extract_theme()
    demographic_data = extract_demographic()

    create_genre_table >> genre_data >> load_genre(genre_data)
    create_theme_table >> theme_data >> load_theme(theme_data)
    create_demographic_table >> demographic_data >> load_demographic(demographic_data)

dag = ProcessAnimeClassification()



