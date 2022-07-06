import pendulum
from airflow import DAG
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
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
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['athletes'],
    dag_id="athletes"
) as dag:
    @task
    def write_topic():
        producer = KafkaProducer(bootstrap_servers='172.17.0.1:9092', api_version=(0, 10, 2))
        val = next(athletes)
        logger.info({val})
        res = producer.send(topic="athletes", value=val)
        logger.info("Wrote to topic athletes.")
        logger.info(res)
    

        # consumer = KafkaConsumer("athletes", group_id="consoomer", bootstrap_servers='172.17.0.1:9092', api_version=(0, 10, 2))
        # consumer.subscribe(["athletes"])
        # logger.info(consumer.poll(timeout_ms=1000))
        # for m in consumer:
        #     logger.info(m.value)
        # consumer.close()
        # logger.info("Message received")

    write_topic()
