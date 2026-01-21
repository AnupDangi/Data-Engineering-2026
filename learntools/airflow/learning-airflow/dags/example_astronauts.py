from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json

@dag(
    dag_id="random_user_pipeline",
    start_date=datetime(2025, 1, 21),
    schedule="@hourly",
    catchup=False,
    default_args={
        "owner": "data_engineer",
        "retries": 2, ## if fails retry 2 times means per task retry 2 times in each dag run
        "retry_delay": timedelta(minutes=1)
    }
)
def random_user_pipeline():

    @task
    def fetch_user():
        response = requests.get("https://randomuser.me/api/", timeout=10)
        return response.json()

    @task
    def transform_user(raw):
        user = raw["results"][0]
        return {
            "name": f"{user['name']['first']} {user['name']['last']}",
            "email": user["email"],
            "country": user["location"]["country"],
            "ingested_at": datetime.utcnow().isoformat()
        }

    @task
    def store_user(cleaned):
        with open("/tmp/users.json", "a") as f: ## in append mode if any fails creates a duplicate write
            f.write(json.dumps(cleaned) + "\n")

    store_user(transform_user(fetch_user()))

random_user_pipeline()

## concepts 
"""
    DAG Run vs Task Run 
    Dag Run- One scheduled execution (hourly,daily,manual)
    Task Run- One task inside one DAG run 

    eg : If a Dag runs 24 hours 
    and has 5 tasks inside it ,
    then there will be 1 Dag Run and 5 Task Runs
    and total 24 Dag Runs and 120 Task Runs in 24 hours ie (24*5=120)

"""



