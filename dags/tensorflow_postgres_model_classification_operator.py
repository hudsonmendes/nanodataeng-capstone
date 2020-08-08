import json
import numpy as np
import tensorflow as tf
from tensorflow.keras.preprocessing.text import tokenizer_from_json
from tensorflow.keras.preprocessing.sequence import pad_sequences

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class TensorflowPostgresModelClassificationOperator(BaseOperator):
    """
    Iterates through reviews and classify the review texts,
    updating the 'sentiment_class' column in the facts tables.
    """

    @apply_defaults
    def __init__(
            self,
            model_path: str,
            max_length: int,
            redshift_conn_id: str,
            redshift_db: str,
            redshift_select_table: str,
            redshift_select_col_key: str,
            redshift_select_col_classifying: str,
            redshift_update_table: str,
            redshift_update_col_key: str,
            redshift_update_col_classified: str,
            *args, **kwargs) -> None:
        """
        Creates the Operator.

        Parameters
        ----------
        model_path                      {str} the path in which the model has been saved
        max_length                      {str} maximum number of tokens that the model can process
        redshift_conn_id                {str} Airflow connection id to Redshift
        redshift_db                     {str} The name of the Database of the Data Warehouse
        redshift_select_table           {str} The table from which we will select the reviews
        redshift_select_col_key         {str} The name of the id column in the reviews table
        redshift_select_col_classifying {str} The name of the text column in the reviews table
        redshift_update_table           {str} The facts table that will get updated by the classification
        redshift_update_col_key         {str} The name of the id column of the facts table
        redshift_update_col_classified  {str} The name of the sentiment_class column of the facts table
        """
        super(TensorflowPostgresModelClassificationOperator, self).__init__(*args, **kwargs)
        self.model_path = model_path
        self.max_length = max_length
        self.redshift_conn_id = redshift_conn_id
        self.redshift_db = redshift_db
        self.redshift_select_table = redshift_select_table
        self.redshift_select_col_key = redshift_select_col_key
        self.redshift_select_col_classifying = redshift_select_col_classifying
        self.redshift_update_table = redshift_update_table
        self.redshift_update_col_classified = redshift_update_col_classified
        self.redshift_update_col_key = redshift_update_col_key

    def execute(self, context):
        """
        Fetch reviews, classify text and update facts.

        Parameters
        ----------
        context {object} Airflow context
        """
        model = tf.saved_model.load(self.model_path)
        tokenizer = self.prepare_tokenizer()
        hook = self.prepare_hook()
        keys, texts, classifications = [], [], []
        cursor = hook.get_cursor()
        try:
            cursor.execute(self.prepare_sql_fetch())
            for key, text in cursor.fetchall():
                keys.append(key)
                texts.append(text)
                classifications.append(0)
        finally:
            cursor.close()

        classifications = self.classify_sentiments(
            model,
            tokenizer,
            texts)

        # we iterate through the classifications and build
        # a list of tuples used that will be used as parameters
        # for the update query
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            update_sql = self.prepare_sql_update()
            params = []
            for i in range(len(keys)):
                key = keys[i]
                classification = classifications[i]
                params.append((classification, key))

                # every 100 entries, runs the command
                if params and i % 100 == 0:
                    cursor.executemany(update_sql, vars_list=params)
                    conn.commit()
                    params.clear()

            # for any residual, runs command
            if params:
                cursor.executemany(update_sql, vars_list=params)
                conn.commit()
                params.clear()
        finally:
            cursor.close()

    def prepare_tokenizer(self):
        """
        Return tokenizer saved after the model training.
        """
        with open(f'{self.model_path}/assets/tokenizer.json', 'r', encoding='utf-8') as tokenizer_file:
            return tokenizer_from_json(json.dumps(json.load(tokenizer_file)))

    def prepare_hook(self):
        """
        Returns the postgres hook, based on the configuration passed into the operator.
        """
        return PostgresHook(
            postgres_conn_id=self.redshift_conn_id,
            schema=self.redshift_db)

    def prepare_sql_fetch(self):
        """
        Prepare the fetch SQL statement.
        """
        sql = []
        sql.append(f'SELECT {self.redshift_select_col_key}, {self.redshift_select_col_classifying}')
        sql.append(f'FROM {self.redshift_select_table}')
        return '\n'.join(sql)

    def prepare_sql_update(self):
        """
        Prepare the update SQL statement.
        """
        sql = []
        sql.append(f'UPDATE {self.redshift_update_table}')
        sql.append(f'SET {self.redshift_update_col_classified} = %s')
        sql.append(f'WHERE {self.redshift_update_col_key} = %s')
        return '\n'.join(sql)

    def classify_sentiments(self, model, tokenizer, texts):
        """
        Classify review text with model and tokenizer trained.

        Parameters
        ----------
        model     {object} tensorflow model trained and saved
        tokenizer {object} keras tokenizer created with the training data
        texts     {list}   list of texts that will be classified
        """
        seqs = tokenizer.texts_to_sequences(texts)
        seqs = pad_sequences(seqs, maxlen=self.max_length, dtype='float32', padding='post', value=0)
        preds = model(inputs=seqs)
        return [-1 if np.argmax(p) else 1 for p in preds]
