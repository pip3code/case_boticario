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
import requests

default_args = {
    'owner': 'Diego Oliveira',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# configuracoes da dag
dag = DAG('dag_ingest_vendas', default_args=default_args, schedule_interval='@0 15 * * *', catchup=False)

# busco as credenciais 
fs = gcsfs.GCSFileSystem(project='caseBoti')
with fs.open('gs://southamerica-east1-composer-20882c00-bucket/credentials.json', 'r') as f:
    credentials = json.load(f)
    clientStorage = storage.Client.from_service_account_info(info=credentials)
    clientBigQuery = bigquery.Client.from_service_account_info(info=credentials)

def read_xls_bucket():
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
    return df

def execute_ingestion():
    dfIngest = pd.DataFrame()
    
    # ingestao dos dados do bucket com retorno em pandas
    dfIngest = read_xls_bucket()

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
        dfIngest, table, job_config=job_config
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


# tasks 
run_ingestion = PythonOperator(
    task_id='run_ingestion',
    python_callable=execute_ingestion,
    dag=dag,
)

run_consolidate_table = PythonOperator(
    task_id='run_consolidate_table',
    python_callable=consolidate_tables,
    dag=dag,
)

# sequencia de execuçao
run_ingestion >> run_consolidate_table