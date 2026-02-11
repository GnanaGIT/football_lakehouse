import subprocess
from prefect import flow, task


@task(retries=2, retry_delay_seconds=30)
def run_gold_job():
    cmd = [
        "python",
        "football-lakehouse/pipelines/gold/gold_fact_player_match.py",
        "--silver-path", "/home/gnana/football-lakehouse/silver",
        "--gold-path", "/home/gnana/football-lakehouse/gold"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Gold job failed")


@flow(name="gold_fact_player_match_flow")
def gold_flow():
    run_gold_job()


if __name__ == "__main__":
    gold_flow()
