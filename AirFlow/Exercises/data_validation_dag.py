from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random


@dag(
   "data_quality_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
    description="Data Quality Check DAG",
)
def data_quality_pipeline():

    CORRECT_PROB = 0.7

    def get_bookings_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/bookings/{file_date}/bookings.json"

    def generate_booking_id(i):
        if random.random() < CORRECT_PROB:
            return i + 1

        return ""

    def generate_listing_id():
        if random.random() < CORRECT_PROB:
            return random.choice([1, 2, 3, 4, 5])

        return ""

    def generate_user_id(correct_prob=0.7):
        return random.randint(1000, 5000) if random.random() < correct_prob else ""

    def generate_booking_time(execution_date):
        if random.random() < CORRECT_PROB:
            return execution_date.strftime('%Y-%m-%d %H:%M:%S')

        return ""

    def generate_status():
        if random.random() < CORRECT_PROB:
            return random.choice(["confirmed", "pending", "cancelled"])

        return random.choice(["unknown", "", "error"])

    @task
    def generate_bookings():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        num_bookings = random.randint(5, 15)
        bookings = []

        for i in range(num_bookings):
            booking = {
                "booking_id": generate_booking_id(i),
                "listing_id": generate_listing_id(),
                "user_id": generate_user_id(),
                "booking_time": generate_booking_time(context["execution_date"]),
                "status": generate_status()
            }
            bookings.append(booking)

        directory = os.path.dirname(booking_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(booking_path, "w") as f:
            json.dump(bookings, f, indent=4)

        print(f"Written to file: {booking_path}")

    def get_anomalies_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/anomalies/{file_date}/anomalies.json"

    @task
    def quality_check():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        anomalies = []
        valid_statuses = {"confirmed", "pending", "cancelled"}

        with open(booking_path, "r") as f:
            bookings = json.load(f)

        for index, row in enumerate(bookings):
            row_anomalies = []
            if not row["booking_id"]:
                row_anomalies.append("Missing booking_id")
            if not row["listing_id"]:
                row_anomalies.append("Missing listing_id")
            if not row["user_id"]:
                row_anomalies.append("Missing user_id")
            if not row["booking_time"]:
                row_anomalies.append("Missing booking_time")
            if not row["status"]:
                row_anomalies.append("Missing status")


            if row["status"] and row["status"] not in valid_statuses:
                row_anomalies.append(f"Invalid status: {row['status']}")

            if row_anomalies:
                anomalies.append({
                    "booking_id": index,
                    "anomalies": row_anomalies,
                })

        anomalies_file = get_anomalies_path(context)
        directory = os.path.dirname(anomalies_file)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(anomalies_file, "w") as f:
            json.dump(anomalies, f, indent=4)

        print(f"Completed validation for {booking_path}. Anomalies found: {len(anomalies)}")
        print(f"Result written to {anomalies_file}")

    generate_bookings() >> quality_check()

dag_instance = data_quality_pipeline()
