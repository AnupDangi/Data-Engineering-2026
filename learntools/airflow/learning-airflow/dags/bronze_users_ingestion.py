# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# import requests
# import json
# import os

# DEFAULT_ARGS = {
#     "owner": "data_engineer", 
#     "retries": 3,
#     "retry_delay": timedelta(minutes=1),
# }

# @dag(
#     dag_id="minutely_bronze_users_ingestion",
#     start_date=datetime(2026, 1, 21),
#     schedule="* * * * *",  # every minute
#     catchup=False,
#     default_args=DEFAULT_ARGS,
#     tags=["bronze", "ingestion"],
# )

# def minutely_bronze_users_ingestion():

#     @task
#     def fetch_users():
#         response = requests.get(
#             "https://randomuser.me/api/",
#             timeout=10
#         )
#         response.raise_for_status()
#         return response.json()

#     @task
#     def write_to_bronze(raw_data, **context):
#         logical_date = context["data_interval_start"].date()
#         run_id = context["run_id"]

#         base_path = f"/tmp/data/bronze/users/dt={logical_date}/run_id={run_id}"
#         os.makedirs(base_path, exist_ok=True)

#         file_path = f"{base_path}/users.json"

#         # Idempotent write: overwrite for same run_id
#         with open(file_path, "w") as f:
#             json.dump(raw_data, f)

#         return {
#             "file_path": file_path,
#             "logical_date": str(logical_date),
#             "run_id": run_id
#         }

#     write_to_bronze(fetch_users())

# minutely_bronze_users_ingestion()


from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import os

DEFAULT_ARGS = {
    "owner": "data_engineer", 
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="minutely_bronze_users_ingestion",
    start_date=datetime(2026, 1, 21),
    schedule="* * * * *",  # every minute
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "ingestion"],
)

def minutely_bronze_users_ingestion():

    @task
    def fetch_users():
        response = requests.get(
            "https://randomuser.me/api/",
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    @task
    def write_to_bronze(raw_data, **context):
        logical_date = context["data_interval_start"].date()
        run_id = context["run_id"]

        base_path = f"/tmp/data/bronze/users/dt={logical_date}/run_id={run_id}"
        os.makedirs(base_path, exist_ok=True)

        file_path = f"{base_path}/users.json"

        # Idempotent write: overwrite for same run_id
        with open(file_path, "w") as f:
            json.dump(raw_data, f)

        return {
            "file_path": file_path,
            "logical_date": str(logical_date),
            "run_id": run_id
        }

    write_to_bronze(fetch_users())

minutely_bronze_users_ingestion()