from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# The postgres connection id
postgres_conn_id = 'postgres'


# Define the function to extract and transform the data
def extract_transform(**kwargs):
    import pandas as pd
    url = 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'
    data1 = pd.read_html(url)[2]
    df=pd.DataFrame(data1) 
    df['Stadium'] = df['Stadium'].apply(lambda x: re.sub(r'[^A-Za-z0-9\s]', '', x))
    df['Seating capacity'] = df['Seating capacity'].apply(lambda x: re.sub(r'[^0-9]', '', x))
    df['Seating capacity'] = pd.to_numeric(df['Seating capacity'])
    print(df)


    # Push the DataFrame to Xcom
    kwargs['ti'].xcom_push(key='stadium_data', value=df)
    return df

# Define the function to load the data into the database
def load_data(**kwargs):
    df = kwargs['ti'].xcom_pull(key='stadium_data', task_ids='extract_transform')
    # Load the transformed data into the database.
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

        # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stadiums_pipeline (
    Rank INT,
    Stadium VARCHAR(512),
    Seating_Capacity INT,
    Region VARCHAR(512),
    Country VARCHAR(512),
    City VARCHAR(512),
    Images VARCHAR(512),
    Home_Team VARCHAR(512)
     );
    """)

    
    # Insert transformed data into the table
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO stadiums_pipeline (Rank, Stadium, Seating_Capacity, Region, Country, City, Images, Home_Team)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row['Rank'],
            row['Stadium'],
            row['Seating capacity'],
            row['Region'],
            row['Country'],
            row['City'],
            row['Images'],
            row['Home team(s)']
        ))

    conn.commit()
    cursor.close()

# Define the DAG
with DAG(
    dag_id="stadiums_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Define the tasks
    task1 = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
    )
    task2 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True
    )

# Define the task dependencies
    task1 >> task2
