import pandahouse as ph
import pandas as pd
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

conn_rw = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '',
    'user': '',
    'database': 'test'
}

default_args = {
    'owner': 'i.arkhincheev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 12)
}

@dag(default_args=default_args, schedule_interval='0 6 * * *', catchup=False)
def etl_i_arkhincheev():
    
    @task()
    def extract_feed():
        q = """
            SELECT
                toDate(time) as event_date,
                user_id,
                multiIf(age < 20, '0-19', age >= 20 and age < 25, '20-24', age >= 25 and age < 30, '25-29', age >= 30 and age < 35, '30-34', age >= 35 and age < 40, '35-39', '40+') as age,
                if(gender=0, 'male', 'female') as gender,
                os,
                countIf(action='view') as views,
                countIf(action='like') as likes
            FROM
                simulator_20241120.feed_actions
            WHERE
                toDate(time) = yesterday()
            GROUP BY
                event_date,
                user_id,
                age,
                gender,
                os
            """
        result = ph.read_clickhouse(q, connection=connection)
        return result
    
    @task()
    def extract_messages():
        q = """
            SELECT
                toDate(t1.time) as event_date,
                t1.user_id,
                multiIf(t1.age < 20, '0-19', t1.age >= 20 and t1.age < 25, '20-24', t1.age >= 25 and t1.age < 30, '25-29', t1.age >= 30 and t1.age < 35, '30-34', t1.age >= 35 and t1.age < 40, '35-39', '40+') as age,
                if(t1.gender=0, 'male', 'female') as gender,
                t1.os as os,
                count(t1.receiver_id) as messages_sent,
                uniqExact(t1.receiver_id) as users_sent,
                toUInt16(avg(t2.messages_received)) as messages_received,
                toUInt16(avg(t2.users_received)) as users_received
            FROM
                simulator_20241120.message_actions t1
            LEFT JOIN
                (
                    SELECT
                        receiver_id,
                        count(user_id) as messages_received,
                        uniqExact(user_id) as users_received
                    FROM
                        simulator_20241120.message_actions
                    WHERE 
                        toDate(time) = yesterday()
                    GROUP BY
                        receiver_id
                ) t2
            ON
                t1.user_id = t2.receiver_id
            WHERE
                toDate(t1.time) = yesterday()
            GROUP BY
                event_date,
                user_id,
                age,
                gender,
                os
            """
        result = ph.read_clickhouse(q, connection=connection)
        return result
    
    @task()
    def merge_dfs(df_feed, df_messages):
        df = pd.merge(df_feed, df_messages, how='outer', on='user_id')
        df = df.fillna(0)
        df['age_x'] = df.apply(lambda row: row['age_y'] if row['age_x'] == 0 else row['age_x'], axis=1)
        df['gender_x'] = df.apply(lambda row: row['gender_y'] if row['gender_x'] == 0 else row['gender_x'], axis=1)
        df['os_x'] = df.apply(lambda row: row['os_y'] if row['os_x'] == 0 else row['os_x'], axis=1)
        df['event_date_x'] = df.apply(lambda row: row['event_date_y'] if row['event_date_x'] == 0 else row['event_date_x'], axis=1)
        df.drop(['event_date_y', 'os_y', 'gender_y', 'age_y'], axis=1, inplace=True)
        df.rename(columns = {'event_date_x':'event_date', 'age_x':'age', 'gender_x':'gender', 'os_x':'os'}, inplace=True)
        result = df.astype({'views':'int','likes':'int','messages_sent':'int','users_sent':'int', 'messages_received':'int','users_received':'int'})
        return result
    
    @task()
    def calc_metrics_by_age(df):
        result = df.groupby(['event_date', 'age'], as_index=False) \
                   .agg({
                            'views': 'sum',
                            'likes': 'sum',
                            'messages_sent': 'sum',
                            'users_sent': 'sum',
                            'messages_received': 'sum',
                            'users_received': 'sum'
                         }) \
                   .rename(columns={'age':'dimension_value'})
        result['dimension'] = 'age'
        return result
    
    @task()
    def calc_metrics_by_gender(df):
        result = df.groupby(['event_date', 'gender'], as_index=False) \
                   .agg({
                            'views': 'sum',
                            'likes': 'sum',
                            'messages_sent': 'sum',
                            'users_sent': 'sum',
                            'messages_received': 'sum',
                            'users_received': 'sum'
                         }) \
                   .rename(columns={'gender':'dimension_value'})
        result['dimension'] = 'gender'
        return result
    
    @task()
    def calc_metrics_by_os(df):
        result = df.groupby(['event_date', 'os'], as_index=False) \
                   .agg({
                            'views': 'sum',
                            'likes': 'sum',
                            'messages_sent': 'sum',
                            'users_sent': 'sum',
                            'messages_received': 'sum',
                            'users_received': 'sum'
                         }) \
                   .rename(columns={'os':'dimension_value'})
        result['dimension'] = 'os'
        return result
    
    @task()
    def insert_into_ch(df):
        context = get_current_context()
        ds = context['ds']
        ph.to_clickhouse(df=df, table='etl_i_arkhincheev', connection=conn_rw, index=False)
        print(f'---insert to clickhouse for {ds}---')
        
    
    df_feed = extract_feed()
    df_messages = extract_messages()
    merged_df = merge_dfs(df_feed, df_messages)
    metrics_by_age = calc_metrics_by_age(merged_df)
    metrics_by_gender = calc_metrics_by_gender(merged_df)
    metrics_by_os = calc_metrics_by_os(merged_df)
    insert_into_ch(metrics_by_age)
    insert_into_ch(metrics_by_gender)
    insert_into_ch(metrics_by_os)
    

etl_dag = etl_i_arkhincheev()