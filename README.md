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

## Project Documentation

Please refer to the [Movie Review Sentiment Analysis Notebook](Movie%20Review%20Sentiment%20Analysis.ipynb#) for end-to-end understanding of the project, as well as setup instructions.