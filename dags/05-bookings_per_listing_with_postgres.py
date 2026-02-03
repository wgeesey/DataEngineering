from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import csv


@dag(
    "bookings_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    description="",
)
def bookings_per_listing_with_sensor():

    @task
    def read_bookings_from_postgres():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")

        file_path = f"/tmp/data/bookings/{file_date}/bookings.csv"

        start_of_minute = execution_date.replace(second=0, microsecond=0)
        end_of_minute = start_of_minute + timedelta(minutes=1)

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        query = f"""
            SELECT booking_id, listing_id, user_id, booking_time, status
            FROM bookings
            WHERE booking_time >= '{start_of_minute.strftime('%Y-%m-%d %H:%M:%S')}'
              AND booking_time < '{end_of_minute.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        records = pg_hook.get_records(query)

        bookings = []

        print(f"Read {len(records)} from Postgres")
        for record in records:
            booking = {
                "booking_id": record[0],
                "listing_id": record[1],
                "user_id": record[2],
                "booking_time": record[3].strftime('%Y-%m-%d %H:%M:%S'),
                "status": record[4]
            }
            bookings.append(booking)

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        fieldnames = ["booking_id", "listing_id", "user_id", "booking_time", "status"]

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for booking in bookings:
                writer.writerow({
                    "booking_id": booking["booking_id"],
                    "listing_id": booking["listing_id"],
                    "user_id": booking["user_id"],
                    "booking_time": booking["booking_time"],
                    "status": booking["status"]
                })

        print(f"Generated bookings data written to {file_path}")

    wait_for_listings_file = FileSensor(
        task_id="wait_for_listings_file",
        fs_conn_id="local_fs",
        filepath="/tmp/data/listings/{{ execution_date.strftime('%Y-%m') }}/listings.csv.gz",
        poke_interval=30,
        timeout=600,
    )

    spark_job = SparkSubmitOperator(
        task_id="process_listings_and_bookings",
        application="bookings_per_listing_spark.py",
        name="listings_bookings_join",
        application_args=[
            "--listings_file", "/tmp/data/listings/{{ execution_date.strftime('%Y-%m') }}/listings.csv.gz",
            "--bookings_file", "/tmp/data/bookings/{{ execution_date.strftime('%Y-%m-%d_%H-%M') }}/bookings.csv",
            "--output_path", "/tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H-%M') }}"
        ],
        conn_id='spark_default',
    )

    bookings_file = read_bookings_from_postgres()
    bookings_file >> spark_job
    wait_for_listings_file >> spark_job

dag_instance = bookings_per_listing_with_sensor()