import pendulum
from airflow import DAG
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata, FutureProduceResult
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task, dag
from datetime import datetime
import os, json, csv
import logging

logger = logging.getLogger()


def read_athletes():
    athletes = open(os.path.join('data', 'olympics', 'olympic_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    header = next(reader, None)
    for athlete in reader:
        yield json.dumps(dict(zip(header, athlete))).encode('utf-8')

athletes = read_athletes()

with DAG(
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['athletes'],
    dag_id="athletes"
) as dag:
    @task
    def write_topic():
        producer = KafkaProducer(bootstrap_servers='kafka:9093', api_version=(0, 10, 2))
        res: FutureRecordMetadata = producer.send(topic="athletes", value=next(athletes))
        logger.info("Wrote to topic athletes.")
        try:
            record = res.get()
            logger.info(record)
        except Exception as e:
            logger.info(e)

    write_topic()
