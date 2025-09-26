from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import clickhouse_connect, json

default_args = {
    "owner": "Sharaf",
    "start_date": datetime(2025, 10, 1),
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "warm_dag",
    default_args=default_args,
    schedule='0 0 * * *'
)

with open("config/clickhouse_config.json") as f:
    conf = json.load(f)

client = clickhouse_connect.get_client(
        host=conf["CLICKHOUSE_HOST"],
        user=conf["CLICKHOUSE_USERNAME"],
        password=conf["CLICKHOUSE_PASSWORD"],
        secure=True
    )

def create_bronze():
    client.query('''CREATE TABLE IF NOT EXISTS Telecom_Warm.call_events_bronze
                    (
                        `call_id` String PRIMARY KEY,
                        `subscribers_id` Int32,
                        `call_start` DateTime,
                        `call_end` DateTime,
                        `duration_sec` Int32,
                        `call_type` String,
                        `tower_id` Int32
                    )
                ''')
    
def insert_truncate():
    client.query('''INSERT INTO Telecom_Warm.call_events_bronze
                    SELECT * FROM Telecom_Hot.call_events_bronze
                ''')
    
    client.query('''TRUNCATE TABLE Telecom_Hot.call_events_bronze''')


def create_silver():
    client.query('''CREATE TABLE IF NOT EXISTS Telecom_Warm.call_events_silver
                    (
                        call_id UUID PRIMARY KEY,
                        subscribers_id Int32,
                        subscriber_name String,
                        plan_type String,
                        call_start DateTime,
                        call_end DateTime,
                        duration_sec Int32,
                        call_type String,
                        tower_id Int32,
                        tower_location String,
                        tower_region String
                    )
                ''')

def insert_silver():
    client.query('''INSERT INTO Telecom_Warm.call_events_silver
                    SELECT 
                        cb.call_id,
                        cb.subscribers_id,
                        s.name,
                        s.plan_type,
                        cb.call_start,
                        cb.call_end,
                        cb.duration_sec,
                        cb.call_type,
                        cb.tower_id,
                        t.location,
                        t.region
                    FROM Telecom_Warm.call_events_bronze cb
                    JOIN CDR.subscribers s ON cb.subscribers_id = s.subscriber_id
                    JOIN CDR.cell_towers t USING(tower_id)
                    WHERE toDate(cb.call_start) = today()
                ''')
    
def create_gold():
    client.query('''CREATE TABLE IF NOT EXISTS Telecom_Warm.call_events_gold
                    (
                        tower_region String,
                        tower_id Int32,
                        call_hour DateTime,
                        total_calls Int32,
                        avg_duration Float32,
                        max_duration Int32,
                        min_duration Int32,
                        unique_subscribers Int32,
                        voice_calls Int32,
                        video_calls Int32,
                        sms_calls Int32
                    )
                    ORDER BY (tower_region, tower_id, call_hour);
                 ''')
    
def insert_gold():
    client.query('''INSERT INTO Telecom_Warm.call_events_gold
                    SELECT 
                        tower_region,
                        tower_id,
                        toStartOfHour(call_start) AS call_hour,
                        COUNT(call_id) AS total_calls,
                        AVG(duration_sec) AS avg_duration,
                        MAX(duration_sec) AS max_duration,
                        MIN(duration_sec) AS min_duration,
                        COUNT(DISTINCT subscribers_id) AS unique_subscribers,
                        COUNTIf(call_type = 'voice') AS voice_calls,
                        COUNTIf(call_type = 'video') AS video_calls,
                        COUNTIf(call_type = 'sms') AS sms_calls
                    FROM Telecom_Warm.call_events_silver
                    WHERE toDate(call_start) = today()
                    GROUP BY tower_region, tower_id, call_hour
                ''')

create_bronze = PythonOperator(
    task_id = 'create_bronze',
    python_callable=create_bronze,
    dag=dag
)

insert_truncate = PythonOperator(
    task_id = 'insert_data',
    python_callable=insert_truncate,
    dag=dag
)

create_silver = PythonOperator(
    task_id = 'create_silver',
    python_callable=create_silver,
    dag=dag
)

insert_silver = PythonOperator(
    task_id = 'insert_silver',
    python_callable=insert_silver,
    dag=dag
)

create_gold = PythonOperator(
    task_id = 'create_gold',
    python_callable=create_gold,
    dag=dag
)

insert_gold = PythonOperator(
    task_id = 'insert_gold',
    python_callable=insert_gold,
    dag=dag
)

create_bronze >> insert_truncate >> create_silver >> insert_silver >> create_gold >> insert_gold