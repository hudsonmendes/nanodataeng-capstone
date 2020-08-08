import zipfile
import json
import pandas as pd
import tensorflow as tf
import numpy as np
from sklearn.model_selection import train_test_split
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class TensorflowReviewClassifierTrainerOperator(BaseOperator):
    """
    DAG Operator responsible for training our Review Sentiment
    Classifier Model using Tensorflow, which will later be used
    to classify reviews present in our Data Warehouse.
    """
    
    @apply_defaults
    def __init__(
            self,
            training_data_path: str,
            output_model_path: str,
            max_length: int = 500,
            vocab_size: int = 10000,
            emb_dims  : int = 64,
            lstm_units: int = 128,
            batch_size: int = 50,
            epochs    : int = 5,
            *args, **kwargs) -> None:
        """
        Initializes the Operator.

        Parameters
        ----------
        training_data_path {str} local path where the training dataset can be found
        output_model_path  {str} local path to where the model will be saved
        max_length         {int} max number of tokens processed by the model, default 500
        vocab_size         {int} size of the vocabulary for which we will train embeddings, default 64
        lstm_units         {int} number of hidden layers in our LSTM layer
        batch_size         {int} size of the mini-batch used in the training
        epochs             {int} number of epochs used to train the model
        """
        super(TensorflowReviewClassifierTrainerOperator, self).__init__(*args, **kwargs)
        self.training_data_path = training_data_path
        self.output_model_path = output_model_path
        self.max_length = max_length
        self.vocab_size = vocab_size
        self.emb_dims = emb_dims
        self.lstm_units = lstm_units
        self.batch_size = batch_size
        self.epochs = epochs

    def execute(self, context):
        """
        Executes the training pipeline, loading the data, creating the tokenizer,
        tokenizing the text, padding the text, encoding the text, running tests and
        labels against the model for a number of epochs, using the number of mini-batches
        finally training a tensorflow model that gets saved into the output_path.

        Parameters
        ----------
        context {object} airflow context
        """
        df = TensorflowReviewClassifierTrainerOperator.read_data(path=self.training_data_path)
        tokenizer = TensorflowReviewClassifierTrainerOperator.create_tokenizer(texts=df.text, vocab_size=self.vocab_size)
        x_raw, y_raw = TensorflowReviewClassifierTrainerOperator.extract_input_and_target(df=df, tokenizer=tokenizer)
        x_padded = TensorflowReviewClassifierTrainerOperator.pad_input(x=x_raw, seq_len=self.max_length)
        y_categorical = TensorflowReviewClassifierTrainerOperator.categorise_target(y=y_raw)
        x_train, x_valid, y_train, y_valid = train_test_split(x_padded, y_categorical, train_size=0.99)
        model = TensorflowReviewClassifierTrainerOperator.create_model(
            vocab_size=self.vocab_size,
            seq_len=self.max_length,
            emb_dims=self.emb_dims,
            lstm_units=self.lstm_units)
        TensorflowReviewClassifierTrainerOperator.train_model(
            model=model,
            train=(x_train, y_train),
            valid=(x_valid, y_valid),
            batch_size=self.batch_size,
            epochs=self.epochs)
        model.save(self.output_model_path)
        TensorflowReviewClassifierTrainerOperator.save_tokenizer(tokenizer=tokenizer, model_folder=self.output_model_path)
    
    @staticmethod
    def read_data(path):
        """
        Reads training data, present in a zip file, into a pandas dataframe and returns it.

        Parameters
        ----------
        path {str} local path to training data zip file
        """
        with zipfile.ZipFile(path) as zip_file:
            df = pd.read_csv(zip_file.open('train.csv'), header=0, error_bad_lines=False)
            return df
        
    @staticmethod
    def create_tokenizer(texts, vocab_size):
        """
        Creates the tokenizer containing a vocabulary fit to the training data text,
        and able to break continuous texts into tokenized (split) sequences encoded
        into the vocabulary index.

        Parameters
        ----------
        vocab_size {int} the fixed number of words that the vocabulary should have
        """
        tokenizer = tf.keras.preprocessing.text.Tokenizer(
            num_words=vocab_size,
            filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
            lower=True,
            split=' ',
            oov_token='<oov>',
            document_count=0)
        tokenizer.fit_on_texts(texts)
        return tokenizer
    
    @staticmethod
    def extract_input_and_target(df, tokenizer):
        """
        Transforms continuous text into encoded sequences of vocabulary indexes.

        Parameters
        ----------
        tokenizer {object} capable of transforing texts to sequences
        """
        x = tokenizer.texts_to_sequences(df.text)
        y = list(df.sentiment)
        return x, y 
    
    @staticmethod
    def pad_input(x, seq_len):
        """
        Whenever the sequences have less than than the max length,
        completes the sequence with padding zeros, so we end up with
        square tensors that can be processed properly by tensorflow.

        Parameters
        ----------
        seq_len {int} the length required for all sequences.
        """
        return tf.keras.preprocessing.sequence.pad_sequences(
            x,
            maxlen=seq_len,
            dtype='int32',
            padding='post',
            value=0)
    
    @staticmethod
    def categorise_target(y):
        """
        Creates a vector with the labels expected in the training/validation
        data, that are used in the training process to tell it what is right,
        and help the model learn from its mistakes.

        Parameters
        ----------
        y {list} list of labels 
        """
        y = np.array(y)
        return y.reshape(y.shape[0], 1, 1)
    
    @staticmethod
    def create_model(vocab_size, seq_len, emb_dims, lstm_units):
        """
        Creates the model architecture that defines the algorithm through which we
        shall process the input tensors to futurely make predictions.

        Parameters
        ----------
        vocab_size {int} the size of the vocabulary, used in the tokenizer, tha defines how many embedding vectors will be trained.
        seq_len    {int} the length of the sequence that defines one of the dimensions of the square tensors that th emodel will process
        emb_dims   {int} the dimension of the embedding vectors
        lstm_units {int} the hidden layers within the LSTM cells
        """
        model = tf.keras.models.Sequential([
            tf.keras.layers.Embedding(input_dim=vocab_size, output_dim=emb_dims, input_length=seq_len, mask_zero=True),
            tf.keras.layers.LSTM(lstm_units, dropout=0.5, recurrent_dropout=0.5),
            tf.keras.layers.Dense(2, activation='softmax')
        ])
        model.compile(optimizer='rmsprop', loss='sparse_categorical_crossentropy', metrics=['acc'])
        model.summary()
        return model
    
    @staticmethod
    def train_model(model, train, valid, batch_size=32, epochs=3):
        """
        Trains the model created.

        Parameters
        ----------
        model {object} the model architecture, untrained
        """
        return model.fit(
            x=train[0], y=train[1],
            batch_size=batch_size,
            epochs=epochs,
            validation_data=valid,
            shuffle=True,
            verbose=2)
        
    @staticmethod
    def save_tokenizer(model_folder, tokenizer):
        """
        In order to run predictions, we will be able to tokenise the
        text of the reviews. Therefore we must save the tokenizer for
        later use. This method does exactly that

        Parameters
        ----------
        tokenizer {object} capable of transforing texts to sequences
        """
        with open(f'{model_folder}/assets/tokenizer.json', 'w+', encoding='utf-8') as file:
            file.write(tokenizer.to_json())