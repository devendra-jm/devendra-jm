import logging
import os
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.task_group import TaskGroup
from airflow_utils.alerting.opsgenie_hook import create_opsgenie_alert
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Operator


default_args = {
    'owner': 'devendra.parhate',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['devendra.parhate@jupiter.money'],
    #'on_failure_callback': create_opsgenie_alert,
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1
}

airflow_env_vars = Variable.get('AIRFLOW_ENV_VARS', deserialize_json=True)
job_args_constants = Variable.get("<<arg-prefix>>_ARG_CONSTANTS", deserialize_json=True)
athena_job_constants = Variable.get("<<arg-prefix>>_ATHENA_CONSTANTS", deserialize_json=True)
job_env_constants = Variable.get("<<arg-prefix>>_ENV_CONSTANTS", deserialize_json=True)

SEGMENT_PUBLISHER_JAR_NAME = job_args_constants.get("segment_publisher_jar_name")
SEGMENT_PUBLISHER_JOB_IMAGE_NAME = job_args_constants.get("segment_publisher_image_name")
SEGMENT_PUBLISHER_JOB_IMAGE_TAG = job_args_constants.get("segment_publisher_image_tag")
SEGMENT_PUBLISHER_MAIN_CLASS = job_args_constants.get("segment_publisher_main_class")

FIELDS_TO_BE_PUBLISHED = job_args_constants.get("fields_to_be_published")
S3_DATA_BUCKET = athena_job_constants.get("s3_data_bucket")
DATA_STORAGE_LOCATION = athena_job_constants.get("<<spark-job-name>>_data_storage_location")

common_task_params = {
    'namespace': airflow_env_vars.get('namespace'),
    'service_account': airflow_env_vars.get('service_account'),
    's3_kms_key': airflow_env_vars.get('s3_kms_key'),
    'job_image': f'{SEGMENT_PUBLISHER_JOB_IMAGE_NAME}:{SEGMENT_PUBLISHER_JOB_IMAGE_TAG}',
    'main_class': SEGMENT_PUBLISHER_MAIN_CLASS,
    'jar_location': f'local:///opt/spark/mainjar/{SEGMENT_PUBLISHER_JAR_NAME}',
    'resources': {
        'driver_cores': int(job_env_constants.get('spark_driver_cores')),
        'driver_memory': job_env_constants.get('spark_driver_memory'),
        'executor_instances': int(job_env_constants.get('spark_executor_instances')),
        'executor_cores': int(job_env_constants.get('spark_executor_cores')),
        'executor_memory': job_env_constants.get('spark_executor_memory')
    },
    'args': {
        'env': airflow_env_vars.get('environment'),
        'fieldsToBePublished': FIELDS_TO_BE_PUBLISHED,
        'eventName': job_args_constants.get("eventName"),
        '<<event-data-location>>': DATA_STORAGE_LOCATION
    }
}


class JobUtils:

    def __init__(self):
        pass

    def get_event_query(self):
        execution_date_macro = "{{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}}"
        next_execution_date_macro = "{{next_execution_date.strftime('year=%Y/month=%m/date=%d/hour=%H')}}"
        s3_bucket = DATA_STORAGE_LOCATION.split("/")[2]
        s3_key = DATA_STORAGE_LOCATION.split(s3_bucket)[1][1:]
        
        unload_query = f"""UNLOAD ($$
            WITH user_list AS (
                <<user_list_query>>    

                group by 1 
            )
            SELECT
                a.customer_user_id AS customerId,
                a.appsFlyerId,
                a.advertisingId,
                a.platform
            FROM (
                SELECT
                    user_list.customer_user_id,
                    appsflyer.appsflyer_id AS appsFlyerId,
                    coalesce(advertising_id,idfa) AS advertisingId, 
                    appsflyer.platform
                FROM
                    user_list
                    JOIN delta_lake.appsflyer_jupiter_in_app_events appsflyer
                ON user_list.customer_user_id = appsflyer.customer_user_id
            ) AS a
            WHERE appsFlyerId is not null
            GROUP By 1, 2, 3, 4
        $$) TO 's3://{s3_bucket}/{s3_key}{next_execution_date_macro}/' IAM_ROLE 'arn:aws:iam::403223234871:role/jm-dsprod-backend-services' FORMAT AS CSV PARALLEL OFF HEADER"""
        return unload_query


with DAG("<<dag_name>>", description='<<formated_name>>',
         start_date=days_ago(1), default_args=default_args,
         schedule_interval="30 0 * * *", catchup=False, tags=['spark', 'segment']) as dag:
         
    event_fetch_job = RedshiftSQLOperator(
        task_id="event_fetch_job",
        sql=JobUtils().get_event_query()
    )

    segment_publisher_job = SparkKubernetesOperator(
        task_id='segment_publisher_job',
        namespace=airflow_env_vars.get('namespace'),
        application_file="spark_application.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        params=common_task_params,
        execution_timeout=timedelta(seconds=3600*4)
    )

    job_sensor_step = SparkKubernetesSensor(
        task_id='spark_monitor',
        namespace=airflow_env_vars.get('namespace'),
        application_name="{{ task_instance.xcom_pull(task_ids='segment_publisher_job')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        execution_timeout=timedelta(seconds=3600*4),
        attach_log=True
    )
    event_fetch_job >> segment_publisher_job >> job_sensor_step
