from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv

@dag(
    dag_id="mssql_extract_test",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
)
def mssql_extract_dag():

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

        path = "~/airflow/data/staging/orders.csv"
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

    transform_task = SparkSubmitOperator(
        task_id="spark_transform_orders",
        application="~/airflow/dags/spark_jobs/transform_orders.py",
        conn_id="spark_default",
        application_args=[
            "--input_path", "~/airflow/data/staging/orders.csv",
            "--output_path", "s3a://my-first-s3-data-lake/WWI/"
        ]
    )

    extract_task >> transform_task

dag = mssql_extract_dag()

