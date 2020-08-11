# Movie Review Sentiment Analysis

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Project Summary

The present project expresses the fact **artist review sentiment** and **film review sentiment**, based on the data provided by **[IMDb](https://www.imdb.com/)** and **[TMDb](https://www.themoviedb.org/)**

The goal of our **Data Pipeline** is to publish a PDF report to **S3** with the following summarised information:

1. Top 10 Films
2. Worst 10 Films
3. Review Sentiment Distibution
4. Top 10 Actors/Actresses in Best Reviewed Films
5. IMDb Average Voting vs TMDb Sentiment Reviews through Years

Our data sources are:

* **[IMDB Large Movie Reviews Sentiment Dataset](https://www.kaggle.com/jcblaise/imdb-sentiments)**
* **[TMDB Movies (2000-2020) with imdb_id](https://www.kaggle.com/hudsonmendes/tmdb-movies-20002020-with-imdb-id)**
* **[TMDB Reviews, movies released from 2000 and 2020](https://www.kaggle.com/hudsonmendes/tmdb-reviews-movies-released-from-2000-and-2020)**
* **[IMDB Cast (Links)](https://datasets.imdbws.com/title.principals.tsv.gz)**
* **[IMDB Cast (Names)](https://datasets.imdbws.com/name.basics.tsv.gz)**

## Complete Technical Specification

Please refer to the [Movie Review Sentiment Analysis Notebook](Movie%20Review%20Sentiment%20Analysis.ipynb#) for end-to-end understanding of the project, as well as setup instructions.

## Schema

### Star Schema: Review Sentiments by Film

![Star Schema: Film Review Sentiments](images/star-film-review-sentiment.png 'Star Schema: Film Review Sentiments')

### Star Schema: Film Review Sentiments by Cast

![Star Schema: Cast Review Sentiments](images/star-cast-film-review-sentiment.png 'Star Schema: Cast Review Sentiments')

## Data Dictionary

### `fact_films_review_sentiments`

| Attribute                	| Type          	| Nullable   	| Value                                                             	|
|--------------------------	|---------------	|------------	|-------------------------------------------------------------------	|
| `date_id`                	| `timestamp`      	| `not null` 	| yyyy-mm-dd, foreign key to `dim_dates`                            	|
| `film_id`                	| `int`         	| `not null` 	| foreign key to `dim_films`                                        	|
| `review_id`              	| `int`         	| `not null` 	| foreign key to `dim_reviews`                                      	|
| `review_sentiment_class` 	| `short`       	| `null`     	| [-1, 1] value representing the sentiment, classified by our model 	|

### `fact_cast_review_sentiments`

| Attribute                	| Type          	| Nullable   	| Value                                                             	|
|--------------------------	|---------------	|------------	|-------------------------------------------------------------------	|
| `date_id`                	| `timestamp`      	| `not null` 	| yyyy-mm-dd, foreign key to `dim_dates`                            	|
| `cast_id`             	| `int`         	| `not null` 	| foreign key to `dim_cast`                                   	        |
| `review_id`              	| `int`         	| `not null` 	| foreign key to `dim_reviews`                                      	|
| `review_sentiment_class` 	| `short`       	| `null`     	| [-1, 1] value representing the sentiment, classified by our model 	|

### `dim_dates`

| Attribute                	| Type          	| Nullable   	| Value                             	|
|--------------------------	|---------------	|------------	|-----------------------------------	|
| `date_id`                	| `timestamp`      	| `not null` 	| primary key                       	|
| `year`                	| `int`         	| `not null` 	| `year` of timestamp in int format 	|
| `month`             		| `int`         	| `not null` 	| `month` of timestamp in int format	|
| `day`              		| `int`         	| `not null` 	| `day` of timestamp in int format  	|

### `dim_films`

| Attribute                	| Type          	| Nullable   	| Value                             	|
|--------------------------	|---------------	|------------	|-----------------------------------	|
| `film_id`                	| `varchar(32)` 	| `not null` 	| `idmb_id`                           	|
| `title`                	| `varchar(256)` 	| `not null` 	| the original title of the film    	|
| `release_year`         	| `int`         	| `not null` 	| `year` in which the film was released	|

### `dim_cast`

| Attribute                	| Type          	| Nullable   	| Value                             	|
|--------------------------	|---------------	|------------	|-----------------------------------	|
| `cast_id`                	| `varchar(32)` 	| `not null` 	| `cast_id` in the IMDB database    	|
| `film_id`                	| `varchar(32)` 	| `not null` 	| `imdb_id`                         	|
| `full_name`            	| `varchar(256)` 	| `not null` 	| the name of the actor or actress   	|

### `dim_reviews`

| Attribute                	| Type          	| Nullable   	| Value                             		|
|--------------------------	|---------------	|------------	|---------------------------------------	|
| `review_id`            	| `varchar(32)` 	| `not null` 	| `review_id` in the TMDb database  		|
| `film_id`                	| `varchar(32)` 	| `not null` 	| `imdb_id`                         		|
| `text`                	| `varchar(32)` 	| `not null` 	| review text for sentiment classification  |

## Choice of Technology

The present solution allows **Data Analysts** to perform roll-ups and drill-downs into film review sentiment facts linked to both films and actors.

Since the raw data provided into S3 and **reported on demand to the Data Analysis Team** who can then perform further analysis on the database, a single write step is required, whereas many reads and aggregations will be performer.

Given those requirements, the choice of technology was the following:

1. **AWS S3** to store the raw files from **IMDb** and **TMDb** films, reviews and casting.

2. **AWS Redshift** to produce a **Data Warehouse** with the required dimension and fact tables.

3. **Tensorflow** to allow us training a model and running the classification

4. **Apache Airflow** to automate our Data Piepeline.

## Scalability

This solution is scalable, under each of the following scenarios, as follows:

### Scenario 1: The data was increased by 100x

The technology employed is extremely elastic. Under 100x the size of the same data:

1. **AWS S3** would hold the files absolutely in the same maner, without any penalty:

```
Q: How much data can I store in Amazon S3?

The total volume of data and number of objects you can store are unlimited. Individual Amazon S3 objects can range in size from a minimum of 0 bytes to a maximum of 5 terabytes. The largest object that can be uploaded in a single PUT is 5 gigabytes. For objects larger than 100 megabytes, customers should consider using the Multipart Upload capability.

Source: https://aws.amazon.com/s3/faqs/
```

2. **AWS Redshift** `COPY` and `INSERT` operations would perform perfectly well, since there is no upper limit for them, and the `INSERT` statements have been done using the `INSERT INTO SELECT FROM` form, that is optimised in redshift:

```
We strongly encourage you to use the COPY command to load large amounts of data. Using individual INSERT statements to populate a table might be prohibitively slow. Alternatively, if your data already exists in other Amazon Redshift database tables, use INSERT INTO SELECT or CREATE TABLE AS to improve performance. For more information about using the COPY command to load tables, see Loading data.

Source: https://docs.aws.amazon.com/redshift/latest/dg/r_INSERT_30.html
```

3. **Table Scanning Operations** like *review classification* are done by running a `SELECT` query in AWS Redshift and then streaming through the results to get classified by the model.

    3.1. **Computer Resources** will not be particularly hit, given the fac that the classification is done by streaming through a small part of the dataset per time.

    3.2. **Time length to Classify Reviews** will increase linearly as the data increases. Some level of parallelisation of this procedure (based on a foor loop) will be required.

4. The **Data Warehouse** has been designed for intesive reading and aggregation on the **fact tables**, and works on files with `json lines` precompiled in S3, and processed in batches.

    4.1. Should **intensive writing** be a requirement, instead of databases, we could have a message/queue based system that (a) would *Write streams of files to s3* and (b) **Use Apache Spark to compile our facts**.


### Scenario 2: The pipelines would be run on a daily basis by 7 am every day

1. The **Data Warehouse Building DAG** would have to be scheduled for 00:00 every day.

2. **Landing times** will be required to be setup with alerts, so that we can keep track of late deliveries.

3. `COPY` and `INSERT` from the **Data Warehouse Building DAG** should be little affected, even if the data increases to 100x, because they currently run within 20 seconds, which in the worst case scenarios giving us a 6h safety window.

4. However, the *Review Classification* part of the DAG, should the data increase sensibly, could take too long to finish. Parallelisation of that task will need to be taken into consideration.

### Secenario 3: The database needed to be accessed by 100+ people.

1. **AWS Redshift** maximum number of concurrent user defined queries is 50, [according to AWS documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html) 

2. However, fact tables are optimised for reading, and the queuing time for each of the 50 concurrent times should be trivial.

3. Therefore, the system would easily sustain a much larger number of users (100+) with great level of fault tolerance.