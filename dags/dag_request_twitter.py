import requests
import os
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import airflow
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import gcsfs
import json
from google.cloud import bigquery
import pandas as pd

# argumentos da dag
default_args = {
    'owner': 'Diego Oliveira',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# configuracoes da dag
dag = DAG('dag_ingest_info_tweets', default_args=default_args, schedule_interval="0 16 * * *", catchup=False)


def consolidate_last_tweets():

    # parametrizaçao da request feita no rapidapi
    url = "https://twitter154.p.rapidapi.com/search/search"

    headers = {
        "X-RapidAPI-Key": "7edab45664msh0762a9021b291fep1fc453jsnee5113646442",
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
    }

    # busco as credenciais
    fs = gcsfs.GCSFileSystem(project='caseBoti')
    with fs.open('gs://southamerica-east1-composer-20882c00-bucket/credentials.json', 'r') as f:
        credentials = json.load(f)
        clientBigQuery = bigquery.Client.from_service_account_info(info=credentials)

    # Instancio os objetos necessarios para a requisicao do big query
    id_projeto = clientBigQuery.project
    credentials = clientBigQuery._credentials

    # Conecta ao BigQuery e faz uma query para obter os dados
    dfLinhaAnoMes = clientBigQuery.query("""
        SELECT * FROM `dadosVendas.tb_agg_consolidado_linha_ano_mes`
    """).to_dataframe()

    # Filtro com os parametros necessarios
    df_filtered = dfLinhaAnoMes[(dfLinhaAnoMes['ano'] == 2019) & (dfLinhaAnoMes['mes'] == 12)]
    df_sorted = df_filtered.sort_values('qtd_venda', ascending=False)

    # Recupero as infos da linha com mais vendas
    linha_mais_vendida = df_sorted.iloc[0]['linha']
    searchList = ['Boticario', linha_mais_vendida]

    # converto o resultado e concateno com a palavra 'Boticario'
    linhatext = linha_mais_vendida
    queryStr = 'Boticario ' + str(linhatext)


    executestartdate = datetime.now()
    last_week = executestartdate - timedelta(days=7)
    data_formatada = last_week.strftime('%Y-%m-%d')


    # parametros do request
    params = {
        "query": str(queryStr),
        "section": "top",
        "start_date": str(data_formatada)
    }

    # request para a api do rapid
    response = requests.get(url, headers=headers, params=params)
    result_json = response.json()
    dfresult = pd.json_normalize(result_json, ['results'])

    # filtro apenas os 50 primeiros resultados e armazeno o username, e o text do twit
    dftoSave = dfresult[['user.username','text']].head(50)

    # realizo a normalizacao das colunas para o bigquery
    dftoSave = dftoSave.rename(columns={
                    "user.username" : "username",
                    "text": "tweet_text"
                    })

    # # Salvo os dados na tabela agg_last_twitts
    dftoSave.to_gbq(destination_table='dadosVendas.tb_agg_last_tweets', project_id=id_projeto, credentials=credentials, if_exists='append')

# taks
run_consolidate_tweets = PythonOperator(
    task_id="run_consolidade_tweets",
    python_callable=consolidate_last_tweets,
    dag=dag,
)

# Sequencia de execuçao
run_consolidate_tweets