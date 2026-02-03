from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv
import random


@dag(
    "bookings_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    description="",
)
def bookings_spark_pipeline():

    @task
    def generate_bookings():
        context = get_current_context()
        execution_date = context["execution_date"]

        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        file_path = f"/tmp/data/bookings/{file_date}/bookings.csv"

        num_bookings = random.randint(30, 50)
        bookings = []
        for i in range(num_bookings):
            booking = {
                "booking_id": random.randint(1000, 5000),
                "listing_id": random.choice([13913, 17402, 24328, 33332, 116268, 117203, 127652, 127860]),
                "user_id": random.randint(1000, 5000),
                "booking_time": execution_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(["confirmed", "cancelled", "pending"])
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

    spark_job = SparkSubmitOperator(
        task_id="process_listings_and_bookings",
        application="bookings_per_listing_spark.py",
        name="listings_bookings_join",
        application_args=[
            "--listings_file", "/tmp/data/listings/{{ execution_date.strftime('%Y-%m') }}/listings.csv.gz",
            "--bookings_file", "/tmp/data/bookings/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}/bookings.csv",
            "--output_path", "/tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}"
        ],
        conn_id='spark_booking',
    )

    bookings_file = generate_bookings()
    bookings_file >> spark_job

dag_instance = bookings_spark_pipeline()