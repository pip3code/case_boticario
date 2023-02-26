import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import gcsfs
import json
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

default_args = {
    'owner': 'Diego Oliveira',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('dag_data_ingest', default_args=default_args, schedule_interval='@daily')

fs = gcsfs.GCSFileSystem(project='caseBoti')
with fs.open('gs://southamerica-east1-composer-20882c00-bucket/credentials.json', 'r') as f:
    credentials = json.load(f)
    clientStorage = storage.Client.from_service_account_info(info=credentials)
    clientBigQuery = bigquery.Client.from_service_account_info(info=credentials)

def execute_ingestion():
    # parametros dos buckets
    bucket_name = 'docs_vendas'
    bucket = clientStorage.get_bucket(bucket_name)

    # Lista os blobs 
    blobs = bucket.list_blobs()

    df = pd.DataFrame()

    # itero sob o objeto do blob para buscar todos os arquivos
    for blob in blobs:
        if blob.name.endswith('.xlsx'):

            # armazeno em uma variavel temporaria
            temp_df = pd.read_excel(blob.download_as_bytes())

            # concateno o resultado a variavel final de output
            df = pd.concat([df, temp_df])


    # Id da minha tabela no meu dataset
    table = 'caseboti.dadosVendas.raw_vendas'

    # Faco a ingestao dos dados do meu dataframe para minha tabela do banco de dados
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('ID_MARCA', 'INTEGER'),
            bigquery.SchemaField('MARCA', 'STRING'),
            bigquery.SchemaField('ID_LINHA', 'INTEGER'),
            bigquery.SchemaField('LINHA', 'STRING'),
            bigquery.SchemaField('DATA_VENDA', 'DATE'),
            bigquery.SchemaField('QTD_VENDA', 'INTEGER'),
        ],
        write_disposition='WRITE_APPEND'
    )
    job = clientBigQuery.load_table_from_dataframe(
        df, table, job_config=job_config
    )

    # Aguardo o carregamento e termino do JOB
    job.result() 

    # CONFIRMACAO DE LINHAS
    print(f'Foram INGERIDAS -- {job.output_rows}  -- linhas para a tabela {table}.')

def consolidate_tables():
   # Conecta ao BigQuery e faz uma query para obter os dados
    dfRAW = clientBigQuery.query("""
        SELECT * FROM `dadosVendas.raw_vendas`
    """).to_dataframe()

    # converto a coluna data_venda para date
    dfRAW['data_venda'] = pd.to_datetime(dfRAW['data_venda'])


    # extrair ano e mês da coluna 'data_venda'
    dfRAW['ano'] = pd.to_datetime(dfRAW['data_venda']).dt.year
    dfRAW['mes'] = pd.to_datetime(dfRAW['data_venda']).dt.month

    # agrupar os dados por ano e mês e somar as vendas em cada grupo
    dfConsolidadoAnoMes = dfRAW.groupby(['ano', 'mes'], as_index=False)['qtd_venda'].sum()

    # ordenar os resultados por ano e mês
    dfConsolidadoAnoMes = dfConsolidadoAnoMes.sort_values(['ano', 'mes'])

    # consolidado Marca e linha
    dfConsolidadoMarcaELinha = dfRAW.groupby(['marca', 'linha'], as_index=False)['qtd_venda'].sum()

    # agrupar os dados por marca, ano e mês e somar as vendas em cada grupo
    dfConsolidadoMarcaAnoMes = dfRAW.groupby(['marca', 'ano', 'mes'], as_index=False)['qtd_venda'].sum()

    # agrupar os dados por linha, ano e mês e somar as vendas em cada grupo
    dfConsolidadoLinhaAnoMes = dfRAW.groupby(['linha', 'ano', 'mes'], as_index=False)['qtd_venda'].sum()


    id_projeto = clientBigQuery.project
    credentials = clientBigQuery._credentials


    # ENVIO DOS DADOS DE INGESTAO PARA O BIGQUERY
    dfConsolidadoAnoMes.to_gbq(destination_table='dadosVendas.tb_agg_consolidado_ano_mes', project_id=id_projeto, credentials=credentials, if_exists='append')

    dfConsolidadoMarcaELinha.to_gbq(destination_table='dadosVendas.tb_agg_consolidado_marca_linha', project_id=id_projeto, credentials=credentials, if_exists='append')

    dfConsolidadoMarcaAnoMes.to_gbq(destination_table='dadosVendas.tb_agg_consolidado_marca_ano_mes', project_id=id_projeto, credentials=credentials, if_exists='append')

    dfConsolidadoLinhaAnoMes.to_gbq(destination_table='dadosVendas.tb_agg_consolidado_linha_ano_mes', project_id=id_projeto, credentials=credentials, if_exists='append')

def consolidate_last_tweets():
    url = "https://twitter154.p.rapidapi.com/search/search"

    headers = {
        "X-RapidAPI-Key": "7edab45664msh0762a9021b291fep1fc453jsnee5113646442",
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
    }

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

    # variavel para recuperar o dia da execuçao
    executestartdate = datetime.now()
    data_formatada = executestartdate.strftime('%Y-%m-%d')

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

    dftoSave = dftoSave.rename(columns={
                    "user.username" : "username",
                    "text": "tweet_text"
                    })

    # # Salvo os dados na tabela agg_last_twitts
    dftoSave.to_gbq(destination_table='dadosVendas.tb_agg_last_tweets', project_id=id_projeto, credentials=credentials) 

run_ingestion = PythonOperator(
    task_id='run_ingestion',
    python_callable=execute_ingestion,
    dag=dag,
)

run_consolidate_table = PythonOperator(
    task_id='run_consolidate_table',
    python_callable=consolidate_tables,
    dag=dag,
    trigger_rule='all_done'
)

run_consolidate_last_tweets = PythonOperator(
    task_id='run_consolidate_last_tweets',
    python_callable=consolidate_last_tweets,
    dag=dag,
)


run_ingestion >> run_consolidate_table >> run_consolidate_last_tweets