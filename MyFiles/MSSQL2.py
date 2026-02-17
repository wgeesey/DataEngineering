from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

import pyodbc
import os
import csv
SPARK_HOME = os.environ['SPARK_HOME']
S3_BUCKET = "my-first-s3-data-lake"
S3_RAW_KEY = "WWI/raw/orders/order_{{ ds_nodash }}.csv"
S3_PARQUET_KEY = "WWI/analytics/orders/"

@dag(
    dag_id="mssql_extract_test2",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
)
def mssql_extract_dag2():

    @task
    def extract_order():
        hook = MsSqlHook(
            mssql_conn_id="mssql_wideworld",
            schema="WideWorldImporters",
            driver="pyodbc"
        )

        query = """
        SELECT TOP 100 o.OrderID, o.OrderDate, c.CustomerName, 
               ol.Quantity, ol.UnitPrice
        FROM Sales.Orders o
        LEFT JOIN Sales.OrderLines ol ON o.OrderID = ol.OrderID
        LEFT JOIN Sales.Customers c ON o.CustomerID = c.CustomerID
        """

        records = hook.get_records(query)
        AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
        path = os.path.join(AIRFLOW_HOME, "data", "staging", "orders.csv")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "order_id", "order_date",
                "customer_name", "quantity",
                "unit_price"
            ])
            writer.writerows(records)

        print(f"CSV created at {path}")
        return path

    extract_task = extract_order()
    
    upload_to_S3 = LocalFilesystemToS3Operator(
        task_id="upload_csv_to_s3",
        filename="{{ task_instance.xcom_pull(task_ids='extract_order') }}",
        dest_key = S3_RAW_KEY,
        dest_bucket = S3_BUCKET,
        aws_conn_id="aws_default",
        replace=True,
    
    )
    
    spark_transform = SparkSubmitOperator(
        task_id="csv_to_parquet",
        application="/home/wgeesey/airflow/dags/spark_jobs/transform_orders2.py",
        conn_id="spark_default",
        application_args=[
            "--input_path", f"s3a://{S3_BUCKET}/{S3_RAW}",
            "--output_path", f"s3a://{S3_BUCKET}/{S3_PARQUET_KEY}"
        ],
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.550.jar",
        template_fields=["application_args"],
    )

    extract_task >> upload_to_S3 >> spark_transform

dag = mssql_extract_dag2()

