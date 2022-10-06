# Load The Dependencies
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

import datetime
import logging

from airflow import models
from airflow.operators import dummy
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryValueCheckOperator, BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers import gcs_to_bigquery


import googleapiclient.discovery

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage import blob

# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    #'email': [''],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

project_id=models.Variable.get('project_id')

esoc_dataset=models.Variable.get('esoc_dataset')
esoc_job_tables=models.Variable.get('esoc_job_tables', deserialize_json=True)
esoc_buckets=models.Variable.get('esoc_buckets', deserialize_json=True) 
esoc_days=models.Variable.get('esoc_days')                

esoc_yyyymm=models.Variable.get('esoc_yyyymm')

today = datetime.datetime.now()
target_date = today - datetime.timedelta(days=int(esoc_days)+1)         # 'esoc_days'일 전의 데이터를 전송하기 위한 날짜 계산
target_date_str = target_date.strftime('%Y%m%d')                        # 'target_date'를 문자열로 치환
target_yyyymm = target_date.strftime('%Y%m')                            # 원본 디렉토리 구조가 년월/일 로 되어있기 때문에 'target_date'를 년월/일 로 분리
target_dd = target_date.strftime('%d')                                  # 원본 디렉토리 구조가 년월/일 로 되어있기 때문에 'target_date'를 년월/일 로 분리





#GCS에서 log파일이 존재하는 경로 찾기
#일별로 생성된 전체 TSOP JOB 마다 각각 로그가 생성되는게 아니여서(ex. JOB 수행 상태는 성공인데 전송된 파일 개수가 0 개인 경우), 
#로그가 생성되어있는 JOB에 대한 gcs 경로를 확인해서 BQ에 넣어주어야함

blobs=[]
gcs_paths=[]

storage_client=storage.Client()

for esoc_bucket in esoc_buckets : 
    blobs.append(storage_client.list_blobs(esoc_bucket, prefix='storage-transfer/logs/transferJobs/', delimiter='/'))
    
for blob in blobs :
    for b in blob :
        print("")
    gcs_paths.append(blob.prefixes)                                     #2차원 배열 같은 형식으로 path list를 저장


# --------------------------------------------------------------------------------
# Set GCP logging
# --------------------------------------------------------------------------------

logger = logging.getLogger('tsop_gcs2bq_composer')

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

with models.DAG(
        'esoc_insert_log_to_bq_test',
        default_args=default_args,
        tags=['esoc_catch_up'],
        schedule_interval=None
        ) as dag:
    # start = dummy.DummyOperator(
    #     task_id='start',
    #     trigger_rule='all_success'
    # )

    # end = dummy.DummyOperator(
    #     task_id='end',
    #     trigger_rule='all_success'
    # )

    #tsop log파일 bq table에 insert
    GCS_to_BQ=[]
    
    i=0
    for job_table, gcs_path, source_bucket in zip(esoc_job_tables, gcs_paths, esoc_buckets) : 
        for path in gcs_path :
            if esoc_yyyymm not in path :
                logger.info("FAIL : This TSOP JOB/Operation hasn't Log.")
            else :
                GCS_to_BQ.append(gcs_to_bigquery.GCSToBigQueryOperator(
                    task_id=f'GCS_to_BQ_{i}',
                    bucket=source_bucket,
                    source_objects=[f'{path}*'],
                    destination_project_dataset_table=f"{project_id}.{esoc_dataset}.{job_table}",
                    source_format='CSV',
                        schema_fields=[
                        {"mode": "NULLABLE", "name": "Timestamp","type": "TIMESTAMP"},
                        {"mode": "NULLABLE", "name": "OperationName", "type": "STRING" },
                        {"mode": "NULLABLE", "name": "Action","type": "STRING"},
                        {"mode": "NULLABLE", "name": "ActionStatus", "type": "STRING"},
                        {"mode": "NULLABLE", "name": "FailureDetails_ErrorType", "type": "STRING" },
                        {"mode": "NULLABLE", "name": "FailureDetails_GrpcCode", "type": "STRING"},
                        {"mode": "NULLABLE", "name": "FailureDetails_Message","type": "STRING" },
                        {"mode": "NULLABLE", "name": "Src_Type", "type": "STRING" },
                        {"mode": "NULLABLE", "name": "Src_File_Path", "type": "STRING" },
                        {"mode": "NULLABLE", "name": "Src_File_LastModified", "type": "INTEGER"},
                        {"mode": "NULLABLE", "name": "Src_File_Size", "type": "INTEGER" },
                        {"mode": "NULLABLE", "name": "Src_File_Crc32C",  "type": "INTEGER" },
                        {"mode": "NULLABLE", "name": "Dst_Type", "type": "STRING" },
                        {"mode": "NULLABLE", "name": "Dst_Gcs_BucketName","type": "STRING" },
                        {"mode": "NULLABLE", "name": "Dst_Gcs_ObjectName", "type": "STRING"},
                        {"mode": "NULLABLE", "name": "Dst_Gcs_LastModified", "type": "INTEGER" },
                        {"mode": "NULLABLE", "name": "Dst_Gcs_Size", "type": "INTEGER"},
                        {"mode": "NULLABLE", "name": "Dst_Gcs_Crc32C", "type": "INTEGER" },
                        {"mode": "NULLABLE", "name": "Dst_Gcs_Md5",  "type": "STRING"},
                        {"mode": "NULLABLE", "name": "Dst_Gcs_PreviousOperation_LastModified", "type": "INTEGER" },
                        {"mode": "NULLABLE", "name": "Dst_Gcs_PreviousOperation_Size", "type": "INTEGER" }
                        ],
                    create_disposition='CREATE_IF_NEEDED',
                    write_disposition='WRITE_APPEND',
                    skip_leading_rows=1,
                    field_delimiter="\t",
                    autodetect=False,
                    location='asia-northeast3'
                ))
                if i > 0 :
                        GCS_to_BQ[i-1] >> GCS_to_BQ[i]
                i+=1    

    #start >> GCS_to_BQ >> end
    # else :
    #     logger.info("FAIL : This TSOP JOB/Operation hasn't scheduled yet.")
    #     start >> end
