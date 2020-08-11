from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from s3_unzip_and_upload_operator import S3UnzipAndUploadOperator
from s3_download_and_upload_operator import S3DownloadAndUploadOperator
from s3_to_redshift_operator import S3ToRedshiftOperator
from tensorflow_postgres_model_classification_operator import TensorflowPostgresModelClassificationOperator
from s3_pdf_sentiment_report_operator import S3PdfSentimentReportOperator
from check_no_missing_id_operator import CheckNoMissingIdOperator

default_args = {
    'owner': 'hudsonmendes',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
    'start_date': datetime.now()
}

dag = DAG(
    'sentiment_classifier_data_warehouse_dag',
    default_args=default_args,
    description='Creates the Data Warehouse',
    schedule_interval='@once',
    max_active_runs=1)

# STEP 0: START ==========================================================
start_operator = DummyOperator(
    task_id='begin_execution', 
    dag=dag)
# ========================================================================

# STEP 1: STAGE IN S3 ====================================================
unzip_movies_to_s3_operator = S3UnzipAndUploadOperator(
    task_id='unzip_movies_to_s3',
    s3_source_url=Variable.get('s3_movies_source_url'),
    s3_destination_bucket=Variable.get('s3_staging_bucket'),
    s3_destination_folder=Variable.get('s3_staging_movies_folder'),
    dag=dag)

unzip_movie_reviews_to_s3_operator =  S3UnzipAndUploadOperator(
    task_id='unzip_movie_reviews_to_s3',
    s3_source_url=Variable.get('s3_movie_reviews_source_url'),
    s3_destination_bucket=Variable.get('s3_staging_bucket'),
    s3_destination_folder=Variable.get('s3_staging_movie_reviews_folder'),
    dag=dag)

transfer_cast_from_http_to_s3_operator = S3DownloadAndUploadOperator(
    task_id='transfer_cast_from_http_to_s3',
    http_source_url=Variable.get('http_cast_source_url'),
    s3_destination_bucket=Variable.get('s3_staging_bucket'),
    s3_destination_path=Variable.get('s3_staging_cast_path'),
    dag=dag)

transfer_cast_names_from_http_to_s3_operator = S3DownloadAndUploadOperator(
    task_id='transfer_cast_names_from_http_to_s3',
    http_source_url=Variable.get('http_cast_names_source_url'),
    s3_destination_bucket=Variable.get('s3_staging_bucket'),
    s3_destination_path=Variable.get('s3_staging_cast_names_path'),
    dag=dag)
# ========================================================================

# STEP 2: STAGE IN REDSHIFT ==============================================
copy_movies_to_redshift_staging_operator = S3ToRedshiftOperator(
    task_id='copy_movies_to_redshift_staging',
    redshift_destination_conn_id='redshift_dw',
    redshift_destination_db=Variable.get('redshift_db'),
    redshift_destination_table_name="staging_tmdb_movies",
    redshift_destination_table_columns={
        'id'               : 'integer',
        'video'            : 'boolean',
        'vote_count'       : 'bigint',
        'vote_average'     : 'numeric(10, 6)',
        'title'            : 'varchar(256)',
        'release_date'     : 'timestamp',
        'original_language': 'varchar(10)',
        'original_title'   : 'varchar(256)',
        'genre_ids'        : 'varchar(1024)',
        'backdrop_path'    : 'varchar(1024)',
        'adult'            : 'boolean',
        'overview'         : 'varchar(10000)',
        'poster_path'      : 'varchar(1024)',
        'popularity'       : 'numeric(10, 6)',
        'id_imdb'          : 'varchar(32)'
    },
    s3_source_bucket=Variable.get('s3_staging_bucket'),
    s3_source_path=Variable.get('s3_staging_movies_folder'),
    iam_role=Variable.get('s3_redshift_iam_role'),
    region=Variable.get('s3_redshift_region'),
    copy_hints=[ 'format', 'as', 'json', "'auto'" ],
    dag=dag)

copy_movie_reviwes_to_redshift_staging_operator = S3ToRedshiftOperator(
    task_id='copy_movie_reviwes_to_redshift_staging',
    redshift_destination_conn_id='redshift_dw',
    redshift_destination_db=Variable.get('redshift_db'),
    redshift_destination_table_name="staging_tmdb_reviews",
    redshift_destination_table_columns={
        'author'  : 'varchar(256)',
        'content' : 'varchar(40000)',
        'id'      : 'varchar(40)',
        'url'     : 'varchar(256)',
        'movie_id': 'integer'
    },
    s3_source_bucket=Variable.get('s3_staging_bucket'),
    s3_source_path=Variable.get('s3_staging_movie_reviews_folder'),
    iam_role=Variable.get('s3_redshift_iam_role'),
    region=Variable.get('s3_redshift_region'),
    copy_hints=[ 'format', 'as', 'json', "'auto'" ],
    dag=dag)

copy_cast_to_redshift_staging_operator = S3ToRedshiftOperator(
    task_id='copy_cast_to_redshift_staging',
    redshift_destination_conn_id='redshift_dw',
    redshift_destination_db=Variable.get('redshift_db'),
    redshift_destination_table_name = "staging_imdb_cast",
    redshift_destination_table_columns = {
        'tconst'    : 'varchar(40)',
        'ordering'  : 'varchar(10)',
        'nconst'    : 'varchar(40)',
        'category'  : 'varchar(256)',
        'job'       : 'varchar(1024)',
        'characters': 'varchar(1024)'
    },
    s3_source_bucket=Variable.get('s3_staging_bucket'),
    s3_source_path=Variable.get('s3_staging_cast_path'),
    iam_role=Variable.get('s3_redshift_iam_role'),
    region=Variable.get('s3_redshift_region'),
    copy_hints=[ 'delimiter', "'\\t'", 'gzip' ],
    dag=dag)

copy_cast_names_to_redshift_staging_operator = S3ToRedshiftOperator(
    task_id='copy_cast_names_to_redshift_staging',
    redshift_destination_conn_id='redshift_dw',
    redshift_destination_db=Variable.get('redshift_db'),
    redshift_destination_table_name = "staging_imdb_names",
    redshift_destination_table_columns = {
        'nconst'           : 'varchar(40)',
        'primaryname'      : 'varchar(256)',
        'birthyear'        : 'varchar(10)',
        'deathyear'        : 'varchar(10)',
        'primaryprofession': 'varchar(256)',
        'knownfortitles'   : 'varchar(256)'
    },
    s3_source_bucket=Variable.get('s3_staging_bucket'),
    s3_source_path=Variable.get('s3_staging_cast_names_path'),
    iam_role=Variable.get('s3_redshift_iam_role'),
    region=Variable.get('s3_redshift_region'),
    copy_hints=[ 'delimiter', "'\\t'", 'gzip' ],
    dag=dag)
# ========================================================================

# STEP 3: INSERTO INTO DIM TABLES ========================================
insert_movies_redshift_dim_operator = PostgresOperator(
    task_id='insert_movies_redshift_dim',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table dim_dates;

        insert into dim_dates ( date_id, year, month, day )
        select release_date as date_id,
            datepart(year, release_date)  as year,
            datepart(month, release_date) as month,
            datepart(day, release_date)   as day
        from (
            select distinct release_date
            from staging_tmdb_movies
            where not release_date is null)""",
    dag=dag)

insert_movie_reviews_to_redshift_dim_operator = PostgresOperator(
    task_id='insert_movie_reviwes_to_redshift_dim',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table dim_films;

        insert into dim_films ( film_id, date_id, title )
        select id, release_date, title
        from staging_tmdb_movies
        where not release_date is null""",
    dag=dag)

insert_cast_to_redshift_dim_operator = PostgresOperator(
    task_id='insert_cast_to_redshift_dim',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table dim_cast;

        insert into dim_cast ( cast_id, film_id, full_name )
        select imdbc.nconst, tmdbm.id, imdbn.primaryname
        from staging_imdb_cast as imdbc
            inner join staging_imdb_names as imdbn on imdbc.nconst = imdbn.nconst
            inner join staging_tmdb_movies as tmdbm on tmdbm.id_imdb = imdbc.tconst
        where not release_date is null
        and imdbc.category in ('actor', 'actress')""",
    dag=dag)

insert_cast_names_to_redshift_dim_operator = PostgresOperator(
    task_id='insert_cast_names_to_redshift_dim',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table dim_reviews;

        insert into dim_reviews ( review_id, film_id, text )
        select id, movie_id, content
        from staging_tmdb_reviews""",
    dag=dag)
# ========================================================================

# STEP 4: PRE-LOAD FACTS TABLES ==========================================
preload_redshift_fact_films_operator = PostgresOperator(
    task_id='preload_redshift_fact_films',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table fact_film_review_sentiments;

        insert into fact_film_review_sentiments (date_id, film_id, review_id, review_sentiment_class)
        select df.date_id, df.film_id, dr.review_id, 0
        from dim_films as df
            inner join dim_reviews as dr on df.film_id = dr.film_id""",
    dag=dag)


preload_redshift_fact_cast_operator = PostgresOperator(
    task_id='preload_redshift_fact_cast',
    postgres_conn_id='redshift_dw',
    database=Variable.get('redshift_db'),
    autocommit=True,
    sql="""
        truncate table fact_cast_review_sentiments;

        insert into fact_cast_review_sentiments (date_id, cast_id, review_id, review_sentiment_class)
        select df.date_id, dc.cast_id, dr.review_id, 0
        from dim_cast as dc
            inner join dim_films as df on dc.film_id = df.film_id
            inner join dim_reviews as dr on dc.film_id = dr.film_id""",
    dag=dag)
# ========================================================================

# STEP 5: RUN REVIEW CLASSIFICATION ======================================

classify_sentiment_redshift_fact_films_operator = TensorflowPostgresModelClassificationOperator(
    task_id='classify_senitment_redshift_fact_films',
    model_path=Variable.get("model_output_path"),
    max_length=int(Variable.get("model_max_length")),
    redshift_conn_id='redshift_dw',
    redshift_db=Variable.get('redshift_db'),
    redshift_select_table='dim_reviews',
    redshift_select_col_key='review_id',
    redshift_select_col_classifying='text',
    redshift_update_table='fact_film_review_sentiments',
    redshift_update_col_classified='review_sentiment_class',
    redshift_update_col_key='review_id',
    dag=dag)

classify_sentiment_redshift_fact_cast_operator = TensorflowPostgresModelClassificationOperator(
    task_id='classify_senitment_redshift_fact_cast',
    model_path=Variable.get("model_output_path"),
    max_length=int(Variable.get("model_max_length")),
    redshift_conn_id='redshift_dw',
    redshift_db=Variable.get('redshift_db'),
    redshift_destination_conn_id='redshift_dw',
    redshift_select_table='dim_reviews',
    redshift_select_col_key='review_id',
    redshift_select_col_classifying='text',
    redshift_update_table='fact_cast_review_sentiments',
    redshift_update_col_classified='review_sentiment_class',
    redshift_update_col_key='review_id',
    dag=dag)

# ========================================================================

# STEP 6: QUALITY CHECKS =================================================

quality_check_no_missing_reviews_in_films_fact_operator = CheckNoMissingIdOperator(
    task_id='quality_check_no_missing_reviews_in_films_fact',
    reference_conn_id='redshift_dw',
    reference_db=Variable.get('redshift_db'),
    reference_ids_sql="SELECT DISTINCT(review_id) FROM dim_reviews",
    checking_conn_id='redshift_dw',
    checking_db=Variable.get('redshift_db'),
    checking_ids_sql="SELECT DISTINCT(review_id) FROM fact_film_review_sentiments",
    dag=dag)

quality_check_no_missing_cast_in_fact_operator = CheckNoMissingIdOperator(
    task_id='quality_check_no_missing_cast_in_fact',
    reference_conn_id='redshift_dw',
    reference_db=Variable.get('redshift_db'),
    reference_ids_sql="SELECT dc.cast_id FROM dim_cast as dc INNER JOIN dim_reviews AS dr ON dc.film_id = dr.film_id",
    checking_conn_id='redshift_dw',
    checking_db=Variable.get('redshift_db'),
    checking_ids_sql="SELECT f.cast_id FROM fact_cast_review_sentiments AS f",
    dag=dag)

# ========================================================================

# STEP 7: UPLOAD REPORT ==================================================

produce_sentiment_report_pdf_operator = S3PdfSentimentReportOperator(
    task_id='produce_sentiment_report_pdf',
    redshift_conn_id='redshift_dw',
    redshift_db=Variable.get("redshift_db"),
    sql_film_sentiments="""
        select df.title, sum(f.review_sentiment_class) as sentiment
        from fact_film_review_sentiments as f
            inner join dim_films as df on f.film_id = df.film_id
        group by df.title
        order by sentiment desc;""",
    sql_year_sentiments="""
        select dt.year, sum(f.review_sentiment_class) as sentiment
        from fact_film_review_sentiments as f
            inner join dim_dates as dt on f.date_id = dt.date_id
        group by dt.year
        order by dt.year asc;""",
    sql_cast_sentiments="""
        select dc.full_name, sum(f.review_sentiment_class) as sentiment
        from fact_cast_review_sentiments f
            inner join dim_cast as dc on f.cast_id = dc.cast_id
        group by dc.full_name
        order by sentiment desc;""",
    path_working_dir=Variable.get('path_working_dir'),
    path_images_dir=Variable.get('path_images_dir'),
    s3_output_bucket=Variable.get('s3_report_bucket'),
    s3_output_folder=Variable.get('s3_report_folder'),
    dag=dag)

# ========================================================================

# STEP 7: END ============================================================

end_operator = DummyOperator(
    task_id='stop_execution',
    dag=dag)

# ========================================================================

start_operator                                  >> unzip_movies_to_s3_operator
start_operator                                  >> unzip_movie_reviews_to_s3_operator
start_operator                                  >> transfer_cast_from_http_to_s3_operator
start_operator                                  >> transfer_cast_names_from_http_to_s3_operator

unzip_movies_to_s3_operator                     >> copy_movies_to_redshift_staging_operator
unzip_movie_reviews_to_s3_operator              >> copy_movies_to_redshift_staging_operator
transfer_cast_from_http_to_s3_operator          >> copy_movies_to_redshift_staging_operator
transfer_cast_names_from_http_to_s3_operator    >> copy_movies_to_redshift_staging_operator

copy_movies_to_redshift_staging_operator        >> copy_movie_reviwes_to_redshift_staging_operator
copy_movie_reviwes_to_redshift_staging_operator >> copy_cast_to_redshift_staging_operator
copy_cast_to_redshift_staging_operator          >> copy_cast_names_to_redshift_staging_operator

copy_cast_names_to_redshift_staging_operator   >> insert_movies_redshift_dim_operator
copy_cast_names_to_redshift_staging_operator   >> insert_movie_reviews_to_redshift_dim_operator
copy_cast_names_to_redshift_staging_operator   >> insert_cast_to_redshift_dim_operator
copy_cast_names_to_redshift_staging_operator   >> insert_cast_names_to_redshift_dim_operator

insert_movies_redshift_dim_operator             >> preload_redshift_fact_films_operator
insert_movie_reviews_to_redshift_dim_operator   >> preload_redshift_fact_films_operator
insert_cast_to_redshift_dim_operator            >> preload_redshift_fact_cast_operator
insert_cast_names_to_redshift_dim_operator      >> preload_redshift_fact_cast_operator

preload_redshift_fact_films_operator            >> classify_sentiment_redshift_fact_films_operator
preload_redshift_fact_cast_operator             >> classify_sentiment_redshift_fact_cast_operator

classify_sentiment_redshift_fact_films_operator >> quality_check_no_missing_reviews_in_films_fact_operator
classify_sentiment_redshift_fact_cast_operator  >> quality_check_no_missing_cast_in_fact_operator

quality_check_no_missing_reviews_in_films_fact_operator >> produce_sentiment_report_pdf_operator
quality_check_no_missing_cast_in_fact_operator          >> produce_sentiment_report_pdf_operator

produce_sentiment_report_pdf_operator >> end_operator