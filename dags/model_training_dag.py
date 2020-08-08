from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from tensorflow_review_classifier_trainer_operator import TensorflowReviewClassifierTrainerOperator

default_args = {
    'owner': 'hudsonmendes',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': datetime.now()
}

dag = DAG(
    'sentiment_classifier_training_dag',
    default_args=default_args,
    description='Trains the Sentiment Classifier',
    schedule_interval='@once',
    max_active_runs=1)

start_operator = DummyOperator(
    task_id='begin_execution', 
    dag=dag)

download_operator = BashOperator(
    task_id = 'download_training_data',
    bash_command = f"curl '{Variable.get('model_training_data_url')}' --output {Variable.get('model_training_data_path')}",
    dag = dag)

training_operator = TensorflowReviewClassifierTrainerOperator(
    task_id='sentiment_classifier_trainer',
    training_data_path=Variable.get("model_training_data_path"),
    output_model_path=Variable.get("model_output_path"),
    max_length=int(Variable.get("model_max_length")),
    vocab_size=int(Variable.get("model_vocab_size")),
    emb_dims=int(Variable.get("model_emb_dims")),
    lstm_units=int(Variable.get("model_lstm_units")),
    batch_size=int(Variable.get("model_training_batch_size")),
    epochs=int(Variable.get("model_training_epochs")),
    dag=dag)

end_operator = DummyOperator(
    task_id='stop_execution',
    dag=dag)

start_operator >> download_operator
download_operator >> training_operator
training_operator >> end_operator