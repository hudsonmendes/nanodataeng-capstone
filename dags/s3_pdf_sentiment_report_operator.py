import os
import time
import boto3
import pandas as pd
from datetime import datetime

from matplotlib import rcParams

from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Image, Table, TableStyle, Spacer, PageBreak
from reportlab.lib.colors import black

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3PdfSentimentReportOperator(BaseOperator):
    """
    Compiles the Data Warehouse information into a sumarised
    PDF report, and uploads it into S3. The report exposes the
    following information:
        1. Top 10 Films
        2. Worst 10 Films
        3. Review Sentiment Distibution
        4. Top 10 Actors/Actresses in Best Reviewed Films
        5. IMDb Average Voting vs TMDb Sentiment Reviews through Years
    """

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id: str,
            redshift_db: str,
            sql_film_sentiments: str,
            sql_year_sentiments: str,
            sql_cast_sentiments: str,
            path_working_dir: str,
            path_images_dir: str,
            s3_output_bucket: str,
            s3_output_folder: str,
            sentiment_scale: float=2.5,
            *args, **kwargs) -> None:
        """
        Creates the Operator.

        Parameters
        ----------
        redshift_conn_id    {str} Airflow connection id to the redshift server
        redshift_db         {str} Database name in Redshift for our Data Warehouse
        sql_film_sentiments {str} SQL statement required to load sentiments from films, summing by sentiment class
        sql_year_sentiments {str} SQL statement required to load sentiments by year, summing by sentiment class
        sql_cast_sentiments {str} SQL statement required to load sentiments by artists, summing by sentiment class
        path_working_dir    {str} Working Directory, with writer access
        path_images_dir     {str} Images Directory, to use as assets in the reports
        s3_output_bucket    {str} The S3 Output Bucket, to where the report will be uploaded
        s3_output_folder    {str} The s3 Output Folder, to where reports will be uploaded
        sentiment_scale     {float} The scale used in the normalisation of sentiments reported.
        """
        super(S3PdfSentimentReportOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_db = redshift_db
        self.sql_film_sentiments = sql_film_sentiments
        self.sql_year_sentiments = sql_year_sentiments
        self.sql_cast_sentiments = sql_cast_sentiments
        self.path_working_dir = path_working_dir
        self.path_images_dir = path_images_dir
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_folder = s3_output_folder
        self.sentiment_scale = sentiment_scale

    def execute(self, context):
        """
        Execute the report creation logic.

        Parameters
        ----------
        context {object} airflow context
        """
        hook = self.prepare_hook()

        film_sentiments = self.collect_sentiments(hook, self.sql_film_sentiments)
        df_films = self.prepare_df_films(film_sentiments)
        df_films_normalised = self.normalize_df_sentiments(df_films)

        cast_sentiments = self.collect_sentiments(hook, self.sql_cast_sentiments)
        df_cast = self.prepare_df_cast(cast_sentiments)
        df_cast_normalised = self.normalize_df_sentiments(df_cast)

        year_sentiments = self.collect_sentiments(hook, self.sql_year_sentiments)
        df_year = self.prepare_df_year(year_sentiments)

        rcParams.update({'figure.autolayout': True})
        film_review_distro_path = self.prepare_film_review_distro_path(df_films)
        year_review_distro_path = self.prepare_year_review_distro_path(df_year)

        title = self.prepare_title()
        top_10_films = self.prepare_top_10_films(df_films_normalised)
        worst_10_films = self.prepare_worst_10_films(df_films_normalised)
        top_10_cast = self.prepare_top_10_cast(df_cast_normalised)

        pdf_path = self.create_pdf(
            title=title,
            df_films=df_films,
            top_10_films=top_10_films,
            worst_10_films=worst_10_films,
            film_review_distro_path=film_review_distro_path,
            top_10_cast=top_10_cast,
            year_review_distro_path=year_review_distro_path)

        s3 = boto3.client('s3')
        s3.upload_file(
            Filename=pdf_path,
            Bucket=self.s3_output_bucket,
            Key=f'{self.s3_output_folder}/{os.path.basename(pdf_path)}')

    def prepare_film_review_distro_path(self, df_films):
        """
        Generates the Review Normal Distribution Histogram Image.

        Parameters
        ----------
        df_films {object} data frame created from the sentiments loaded with the sql_film_sentiments query
        """
        film_review_distro_path = f'{self.path_working_dir}/film_review_distro_fig.png'
        film_review_distro_fig = df_films[['sentiment']] \
            .plot \
            .density(bw_method=1, grid=True, figsize=(5, 3)) \
            .get_figure()
        film_review_distro_fig.savefig(film_review_distro_path, format='png')
        return film_review_distro_path

    def prepare_year_review_distro_path(self, df_year):
        """
        Generates the Review Sentiment over the Years.

        Parameters
        ----------
        df_year {object} data frame created from the sentiments loaded with the sql_year_sentiments query
        """
        year_review_distro_path = f'{self.path_working_dir}/year_review_distro_fig.png'
        year_review_distro_fig = df_year \
            .plot \
            .bar(grid=True, figsize=(5, 3)) \
            .get_figure()
        year_review_distro_fig.savefig(year_review_distro_path, format='png')
        return year_review_distro_path

    def prepare_title(self):
        """
        Generates the title of the PDF.
        """
        year = str(datetime.now().year).rjust(4, '0')
        month = str(datetime.now().month).rjust(2, '0')
        day = str(datetime.now().day).rjust(2, '0')
        title = f'TMDb, Film Review Sentiment Analysis ({year}-{month}-{day})'
        return title
        
    def prepare_df_films(self, film_sentiments):
        """
        Compiles data frame of sentiments by film.

        Parameters
        ----------
        film_sentiments {list} list of tuples (title, sum(sentiment_class))
        """
        df_films = pd.DataFrame(film_sentiments, columns=['title', 'sentiment'])
        return df_films.set_index('title')

    def prepare_df_cast(self, cast_sentiments):
        """
        Compiles data frame of sentiments by artists.

        Parameters
        ----------
        cast_sentiments {list} list of tuples (full_name, sum(sentiment_class))
        """
        df_cast = pd.DataFrame(cast_sentiments, columns=['full_name', 'sentiment'])
        return df_cast.set_index('full_name')

    def prepare_df_year(self, year_sentiments):
        """
        Compiles data frame of sentiments by year.

        Parameters
        ----------
        year_sentiments {list} list of tuples (year, sum(sentiment_class))
        """
        df_year = pd.DataFrame(year_sentiments, columns=['year', 'sentiment'])
        return df_year.set_index('year')

    def normalize_df_sentiments(self, df):
        """
        Scales the sentiment sum to 0~5 scale.

        Parameters
        ----------
        df {object} normalise the sum(sentiment_class) scaling it between 0~5
        """
        df = df.copy()
        df_normalizer = self.sentiment_normalizer(max(df.sentiment))
        df['normalized_sentiment'] = df['sentiment'].map(df_normalizer)
        return df

    def prepare_top_10_films(self, df_films):
        """
        Takes the first 10 films, ordered by the 'normalized_sentiment' desc

        Parameters
        ----------
        df_films {object} the films data frame with columsn 'title', sum('sentiment_class')
        """
        return df_films[['normalized_sentiment']].head(10)

    def prepare_worst_10_films(self, df_films):
        """
        Takes the last 10 film, ordered by the 'normalized_sentiment' asc

        Parameters
        ----------
        df_films {object} the films data frame with columsn 'title', sum('sentiment_class')
        """
        return df_films[['normalized_sentiment']].tail(10)

    def prepare_top_10_cast(self, df_cast):
        """
        Prepare the top 10 cast (artists), ordered by 'normalized_sentiment' desc

        Parameters
        ----------
        df_cast {object} the films data frame with columsn 'full_name', sum('sentiment_class')
        """
        df_cast = df_cast.copy()
        df_cast_normalizer = self.sentiment_normalizer(max(df_cast.sentiment))
        df_cast['normalized_sentiment'] = df_cast['sentiment'].map(df_cast_normalizer)
        return df_cast[['normalized_sentiment']].head(10)

    def sentiment_normalizer(self, max_sentiment):
        """
        Function builder that normalises the sum('sentiment_class') by dividing it by the
        highest sum('sentiment_class').

        Parameters
        ----------
        max_sentiment {int} highest number of sum('sentiment_class')
        """
        scale = self.sentiment_scale
        return lambda x: round(scale + (scale * x / max_sentiment), 2) if max_sentiment else 0

    def prepare_hook(self):
        """
        Returns the postgres hook, based on the configuration passed into the operator.
        """
        return PostgresHook(
            postgres_conn_id=self.redshift_conn_id,
            schema=self.redshift_db)

    def collect_sentiments(self, hook, sql):
        """
        Collect sentimentos from the database, giving an arbitrary query.

        Parameters
        ----------
        hook {object} provides connection to the database
        sql  {string} the arbitrary sql from which we will fetchall()
        """
        cursor = hook.get_cursor()
        try:
            cursor.execute(sql)
            output = [(key, sum_of_sentiments)
                    for key, sum_of_sentiments
                    in cursor.fetchall()]
        finally:
            cursor.close()
        return output

    def create_pdf(
            self,
            title,
            df_films,
            top_10_films,
            worst_10_films,
            film_review_distro_path,
            top_10_cast,
            year_review_distro_path):
        """
        Creates the PDF from the information provided.

        Parameters
        ----------
        title                   {str}    the title of the document
        df_films                {object} the data frame of filmes, containing 'title', sum('sentiment_class')
        top_10_films            {object} the top 10 filmes, ordered by 'normalized_sentiment' desc
        worst_10_films          {object} the top 10 filmes, ordered by 'normalized_sentiment' asc
        film_review_distro_path {str}    path to the film review distibution image
        top_10_cast             {object} the top 10 artists, ordered by 'normalized_sentiment' desc
        year_review_distro_path {str}    path to the year bar chart image
        """
        output_path = f'{self.path_working_dir}/sentiment-{int(time.time())}.pdf'
        doc = SimpleDocTemplate(output_path, pagesize=A4, rightMargin=cm,
                                leftMargin=cm, topMargin=cm, bottomMargin=cm)
        doc.title = title
        width, _ = A4

        style_title = getSampleStyleSheet()["title"]
        style_h1 = getSampleStyleSheet()["h1"]
        style_normal = getSampleStyleSheet()["bu"]
        style_grid = TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, black),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT')])

        br = Spacer(width, 20)

        elements = []
        elements.append(Paragraph(title, style=style_title))
        elements.append(Image(f'{self.path_images_dir}/header.png', width-(2*cm), 220))
        elements.append(br)

        elements.append(Paragraph('Executive Summary', style=style_h1))
        elements.append(Paragraph(f'The top film in our database, accorindg to TMDB reviews is <strong>{df_films.head(1).index[0]}</strong>', style=style_normal))
        elements.append(br)

        elements.append(Paragraph('Top 10 Films', style=style_h1))
        elements.append(Paragraph('Here are the top 10 films in our database, according to the sentiment found in the TMDb reviews, ranging from 0 (negative) to 5 (positive).', style=style_normal))
        elements.append(Table(top_10_films.copy().reset_index().to_numpy().tolist(), style=style_grid))
        elements.append(br)

        elements.append(Paragraph('Worst 10 Films', style=style_h1))
        elements.append(Paragraph('Here are the worst 10 films in our database, according to the sentiment found in the TMDb reviews, ranging from 0 (negative) to 5 (positive).', style=style_normal))
        elements.append(Table(worst_10_films.copy().reset_index().to_numpy().tolist(), style=style_grid))
        elements.append(br)

        elements.append(Paragraph('Review Sentiment Distibution', style=style_h1))
        elements.append(Image(film_review_distro_path))
        elements.append(br)

        elements.append(Paragraph('Top 10 Actors/Actresses in Best Reviewed Films', style=style_h1))
        elements.append(Paragraph('The ranking bellow is of actors that worked in films with positive reviews. Reviews are not made directly to actors, but to their films', style=style_normal))
        elements.append(Table(top_10_cast.copy().reset_index().to_numpy().tolist(), style=style_grid))
        elements.append(br)

        elements.append(Paragraph('IMDb Average Voting vs TMDb Sentiment Reviews', style=style_h1))
        elements.append(Image(year_review_distro_path))

        doc.build(elements)
        return output_path
