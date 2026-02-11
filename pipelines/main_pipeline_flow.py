import subprocess
from prefect import flow, task

SILVER_PATH = "/home/gnana/football-lakehouse/silver"
GOLD_PATH = "/home/gnana/football-lakehouse/gold"
BRONZE_PATH = "/home/gnana/football-lakehouse/bronze"

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Command failed")

@task(retries=2, retry_delay_seconds=30)
def run_bronze():
    run_cmd([
        "python",
        "pipelines/bronze/bronze_ingest.py",
        "--bronze-path", BRONZE_PATH
    ])

@task(retries=2, retry_delay_seconds=30)
def run_silver():
    run_cmd([
        "python",
        "pipelines/silver/silver_transform.py",
        "--bronze-path", BRONZE_PATH,
        "--silver-path", SILVER_PATH
    ])

@task(retries=2, retry_delay_seconds=30)
def run_gold():
    run_cmd([
        "python",
        "pipelines/gold/gold_fact_player_match.py",
        "--silver-path", SILVER_PATH,
        "--gold-path", GOLD_PATH
    ])

@flow(name="unified_lakehouse_pipeline")
def main_pipeline():
    run_bronze()
    run_silver()
    run_gold()

if __name__ == "__main__":
    main_pipeline()
