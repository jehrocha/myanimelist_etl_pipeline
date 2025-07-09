from datetime import timedelta
import time
import pendulum

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    start_date=pendulum.datetime(2025, 7, 1, tz = "Asia/Manila"),
    catchup=False,
    schedule=timedelta(days=1),
    tags=["anime"],
    dagrun_timeout=timedelta(minutes=60),
)
def ProcessAnimeData():
    create_fact_table = SQLExecuteQueryOperator(
        task_id="create_fact_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS anime_fact_table(
                anime_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                episodes SMALLINT CHECK(episodes >= 0),
                status VARCHAR(20),
                score NUMERIC(3,2) CHECK (score BETWEEN 0 AND 10),
                rank SMALLINT CHECK (rank > 0),
                popularity SMALLINT CHECK (popularity > 0),
                members INTEGER CHECK (members >= 0),
                synopsis TEXT
            );
        """,
    )

    create_dim_table = SQLExecuteQueryOperator(
        task_id="create_dim_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS anime_dim_table(
                anime_id INTEGER PRIMARY KEY REFERENCES anime_fact_table(anime_id) ON DELETE CASCADE,
                type_id SMALLINT REFERENCES type(type_id),
                source_id SMALLINT REFERENCES source(source_id),
                season_id SMALLINT REFERENCES season(season_id),
                year SMALLINT CHECK (year >= 1917)
            );
        """,
    )

    create_temp_type_table = SQLExecuteQueryOperator(
        task_id="create_temp_type_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS temp_type(
                type_id SERIAL PRIMARY KEY,
                type VARCHAR(15) UNIQUE
            );
        """
    )

    create_type_table = SQLExecuteQueryOperator(
        task_id="create_type_table",
        conn_id="anime_list_conn",
        sql = """
            CREATE TABLE IF NOT EXISTS type(
                type_id SERIAL PRIMARY KEY,
                type VARCHAR(15) UNIQUE
            );
        """,
    )
    
    create_temp_source_table = SQLExecuteQueryOperator(
        task_id="create_temp_source_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS temp_source(
                source_id SERIAL PRIMARY KEY,
                source VARCHAR(20) UNIQUE
            );
        """,
    )

    create_source_table = SQLExecuteQueryOperator(
        task_id="create_source_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS source(
                source_id SERIAL PRIMARY KEY,
                source VARCHAR(20) UNIQUE
            );
        """,
    )

    create_season_table = SQLExecuteQueryOperator(
        task_id="create_season_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS season(
                season_id SERIAL PRIMARY KEY,
                season VARCHAR(6) UNIQUE
            );

            INSERT INTO season (season) 
            VALUES ('Winter'), ('Spring'), ('Summer'), ('Fall')
            ON CONFLICT (season) DO NOTHING;
        """,
    )
    
    create_temp_studio_table = SQLExecuteQueryOperator(
        task_id="create_temp_studio_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS temp_studio(
                studio_id SERIAL PRIMARY KEY,
                studio TEXT UNIQUE
            );
        """,
    )

    create_studio_table = SQLExecuteQueryOperator(
        task_id="create_studio_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS studio(
                studio_id SERIAL PRIMARY KEY,
                studio TEXT UNIQUE
            );
        """,
    )

    create_anime_studio_table = SQLExecuteQueryOperator(
        task_id="create_anime_studio_table",
        conn_id="anime_list_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS anime_studio(
                anime_id INTEGER REFERENCES anime_fact_table(anime_id) ON DELETE CASCADE,
                studio_id SMALLINT REFERENCES studio(studio_id),
                PRIMARY KEY(anime_id, studio_id)
            );
        """,
    )

    create_anime_genre_table = SQLExecuteQueryOperator(
        task_id = "create_anime_genre_table",
        conn_id = "anime_list_conn",
        sql = """
            CREATE TABLE IF NOT EXISTS anime_genre(
                anime_id INTEGER REFERENCES anime_fact_table(anime_id) ON DELETE CASCADE,
                genre_id SMALLINT REFERENCES genre(genre_id),
                PRIMARY KEY(anime_id, genre_id)
            );
        """,
    )

    create_anime_theme_table = SQLExecuteQueryOperator(
        task_id  = "create_anime_theme_table",
        conn_id = "anime_list_conn",
        sql = """
            CREATE TABLE IF NOT EXISTS anime_theme(
                anime_id INTEGER REFERENCES anime_fact_table(anime_id) ON DELETE CASCADE,
                theme_id SMALLINT REFERENCES theme(theme_id),
                PRIMARY KEY(anime_id, theme_id)
            )    
        """
    )

    create_anime_demographic_table = SQLExecuteQueryOperator(
        task_id = "create_anime_demographic_table",
        conn_id = "anime_list_conn",
        sql = """
            CREATE TABLE IF NOT EXISTS anime_demographic(
                anime_id INTEGER REFERENCES anime_fact_table(anime_id) ON DELETE CASCADE,
                demographic_id SMALLINT REFERENCES demographic(demographic_id),
                PRIMARY KEY(anime_id, demographic_id)
            )
        """
    )

    @task
    def get_last_page():
        return int(Variable.get("last_page", default_var=1))
    
    @task(multiple_outputs=True)
    def fetch_data(page):
        data = {
            "anime_list" : [],
            "dim_list" : [],
            "anime_genre" : [],
            "anime_theme" : [],
            "anime_demographic" : [],
            "studio_list" : []            
        }
       
        url = "https://api.jikan.moe/v4/anime/"
        daily_limit = 5

        while daily_limit > 0:

            response = requests.get(url, params = {"page" : page})

            if response.status_code == 200:

                response_json = response.json()
                anime_page = response_json.get("data")

                for anime in anime_page:

                    anime_id = anime.get("mal_id")
                    title = anime.get("english_title") or anime.get("title")
                    episodes = anime.get("episodes") or 0
                    status = anime.get("status")
                    score = anime.get("score") or None
                    rank = anime.get("rank")
                    popularity = anime.get("popularity")
                    members = anime.get("members")   
                    synopsis = anime.get("synopsis")

                    data["anime_list"].append((anime_id, title, episodes, status, score, rank, popularity, members, synopsis))

                    anime_type = anime.get("type") or None
                    source = anime.get("source") or None
                    aired = anime.get("aired")
                    year = aired["prop"]["from"].get("year")

                    if not anime.get("season"):
                        month = aired["prop"]["from"].get("month")
                        season = ["Winter", "Spring", "Summer", "Fall"][(month - 1) // 3]
                    else:
                        season = anime.get("season").capitalize()

                    data["dim_list"].append((anime_id, anime_type, source, season, year))          

                    genres = anime.get("genres")
                    if genres:
                        for genre in genres:
                            genre_name = genre.get("name")
                            data["anime_genre"].append((anime_id, genre_name))

                    e_genres = anime.get("explicit_genres")
                    if e_genres:
                        for genre in e_genres:
                            genre_name = genre.get("name")
                            data["anime_genre"].append((anime_id, genre_name))

                    studios = anime.get("studios")
                    if studios:
                        for studio in studios:
                            studio_name = studio.get("name")
                            data["studio_list"].append((anime_id, studio_name))

                    themes = anime.get("themes")
                    if themes:
                        for theme in themes:
                            theme_name = theme.get("name")
                            data["anime_theme"].append((anime_id, theme_name))

                    demographics = anime.get("demographics")
                    if demographics:
                        for demographic in demographics:
                            demographic_name = demographic.get("name")
                            data["anime_demographic"].append((anime_id, demographic_name))
                            
            else:
                print(f"No data found on page {page}. Ending Fetch")
                break

            page += 1
            daily_limit -= 1
            time.sleep(1)

        Variable.set("last_page", str(page))
        return data
    
    
    @task
    def insert_anime_fact_data(data):
        insert_query = """
            INSERT INTO anime_fact_table (anime_id, title, episodes, status, score, rank, popularity, members, synopsis)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (anime_id) DO NOTHING;
        """
        
        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            cur.executemany(insert_query, data)

            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise

    @task
    def insert_dimension(dim_data):
        for anime in dim_data:

            anime_id, anime_type, source, season, year = anime
            
            fetch_query = """
                SELECT %s, type_id, source_id, season_id, %s
                FROM type t, source src, season s
                WHERE type = %s AND source = %s AND season = %s
            """

            insert_query = """
                INSERT INTO anime_dim_table (anime_id, type_id, source_id, season_id, year)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (anime_id) DO NOTHING;
            """

            try:
                postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
                conn = postgres_hook.get_conn()
                cur = conn.cursor()
                
                cur.execute(fetch_query, (anime_id, year, anime_type, source, season))
                result = cur.fetchone()                    
                cur.execute(insert_query, result)

                conn.commit()

            except Exception as e:
                print(f"An unexpected error occured: {e}")
                raise

    @task
    def insert_type(dim_list):
        type_list = []
        for anime in dim_list:
            if (anime[1],) not in type_list:
                type_list.append((anime[1],))

        insert_temp = """
            INSERT INTO temp_type (type)
            VALUES (%s)
            ON CONFLICT (type) DO NOTHING;
        """

        insert_query = """
            INSERT INTO type (type)
            SELECT type FROM temp_type
            WHERE type NOT IN (SELECT type FROM type);
        """

        drop_temp_type = """
            DROP TABLE temp_type
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id = "anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            cur.executemany(insert_temp, type_list)
            cur.execute(insert_query)
            cur.execute(drop_temp_type)

            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise
    
    @task
    def insert_source(dim_list):
        source_list = []
        for anime in dim_list:
            if (anime[2],) not in source_list:
                source_list.append((anime[2],))

        insert_temp = """
            INSERT INTO temp_source (source)
            VALUES (%s)
            ON CONFLICT (source) DO NOTHING;
        """

        insert_query = """
            INSERT INTO source (source)
            SELECT source FROM temp_source
            WHERE source NOT IN (SELECT source FROM source);
        """

        drop_temp_source = """
            DROP TABLE temp_source
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            cur.executemany(insert_temp, source_list)
            cur.execute(insert_query)
            cur.execute(drop_temp_source)

            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")       
            raise
     
    @task
    def insert_studio(studio_list):
        insert_temp = """
            INSERT INTO temp_studio (studio)
            VALUES (%s)
            ON CONFLICT (studio) DO NOTHING;
        """

        insert_query = """
            INSERT INTO studio(studio)
            SELECT studio FROM temp_studio
            WHERE studio NOT IN (SELECT studio FROM studio)
        """

        drop_temp_studio = """
            DROP TABLE temp_studio
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            for studios in studio_list:
                studio_name = studios[1]
                cur.execute(insert_temp, (studio_name,))
        
            cur.execute(insert_query)
            cur.execute(drop_temp_studio)
            
            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise
    
    @task
    def insert_anime_studio(studio_list):
        select_query = """
            SELECT studio_id FROM studio
            WHERE studio = %s;
        """

        insert_query = """
            INSERT INTO anime_studio (anime_id, studio_id)
            VALUES (%s, %s)
            ON CONFLICT (anime_id, studio_id) DO NOTHING;
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            for studio in studio_list:
                anime_id, studio_name = studio
                cur.execute(select_query, (studio_name,))
                studio_id = cur.fetchone()
                cur.execute(insert_query, (anime_id, studio_id))

            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise
    
    @task
    def insert_anime_genre(anime_genre):
        select_query = """
            SELECT genre_id FROM genre
            WHERE genre = %s;
        """

        insert_query = """
            INSERT INTO anime_genre(anime_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT (anime_id, genre_id) DO NOTHING;    
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id = "anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            for genres in anime_genre:
                anime_id, genre_name = genres
                cur.execute(select_query, (genre_name,))
                genre_id = cur.fetchone()
                cur.execute(insert_query, (anime_id, genre_id))
            
            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise
    
    @task
    def insert_anime_theme(anime_theme):
        select_query = """
            SELECT theme_id FROM theme
            WHERE theme = %s;
        """

        insert_query = """
            INSERT INTO anime_theme (anime_id, theme_id)
            VALUES (%s, %s)
            ON CONFLICT (anime_id, theme_id) DO NOTHING;
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id = "anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            for themes in anime_theme:
                anime_id, theme_name = themes
                cur.execute(select_query, (theme_name,))
                theme_id = cur.fetchone()
                cur.execute(insert_query, (anime_id, theme_id))
            
            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise

    @task
    def insert_anime_demographic(anime_demographic):
        select_query = """
            SELECT demographic_id FROM demographic
            WHERE demographic = %s;
        """

        insert_query = """
            INSERT INTO anime_demographic (anime_id, demographic_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id = "anime_list_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            for demographics in anime_demographic:
                anime_id, demographic_name = demographics
                cur.execute(select_query, (demographic_name,))
                demographic_id = cur.fetchone()
                cur.execute(insert_query, (anime_id, demographic_id))
            
            conn.commit()

        except Exception as e:
            print(f"An unexpected error occured: {e}")
            raise


    page = get_last_page()
    data = fetch_data(page)
    anime_list = data["anime_list"]
    dim_list = data["dim_list"]
    anime_genre = data["anime_genre"]
    studio_list = data["studio_list"]
    anime_theme = data["anime_theme"]
    anime_demographic = data["anime_demographic"]

    [create_temp_type_table, create_type_table,  create_temp_source_table, create_source_table, create_season_table, create_fact_table] >> create_dim_table >> data

    create_fact_table >> [create_anime_genre_table, create_anime_theme_table, create_anime_demographic_table, create_anime_studio_table]

    [create_studio_table, create_temp_studio_table] >> create_anime_studio_table >> insert_studio(studio_list) >> insert_anime_studio(studio_list)
     
    create_anime_genre_table >> insert_anime_genre(anime_genre)
    
    create_anime_theme_table >> insert_anime_theme(anime_theme)

    create_anime_demographic_table >> insert_anime_demographic(anime_demographic)

    insert_anime_fact_data(anime_list)

    [insert_type(dim_list), insert_source(dim_list)] >> insert_dimension(dim_list)

dag = ProcessAnimeData()


