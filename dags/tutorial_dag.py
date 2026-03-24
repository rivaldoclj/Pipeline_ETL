from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(response.json())
    qtd = len(df.index)
    return qtd

def e_valido(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if (qtd > 1000):
        return 'valido'
    return 'invalido'

with DAG('tutorial_dag', start_date = datetime(2021, 3, 24),
         schedule = '30 * * * *', catchup = False) as dag:
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )

    e_valido = BranchPythonOperator(
        task_id = 'e_valido',
        python_callable = e_valido
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )

    invalido = BashOperator(
        task_id = "invalido",
        bash_command = "echo 'Quantidade não OK'"
    )

    captura_conta_dados >> e_valido >> [valido, invalido]