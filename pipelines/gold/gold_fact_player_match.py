#!/usr/bin/env python3
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions

def get_spark():
    return SparkSession.builder.getOrCreate()

def load_silver(spark, silver_path):
    players = spark.read.parquet(f"{silver_path}/players_silver")
    games = spark.read.parquet(f"{silver_path}/games_silver")
    clubs = spark.read.parquet(f"{silver_path}/clubs_silver")
    appearances = spark.read.parquet(f"{silver_path}/appearances_silver")
    return players, games, clubs, appearances

def build_fact(players, games, apps, clubs):
    fact = (
        apps
        .join(players, "player_id", "left")
        .join(games, "game_id", "left")
        .join(clubs, players.current_club_id == clubs.club_id, "left")
        .select(
            apps.appearance_id,
            apps.player_id,
            apps.game_id,
            players.name.alias("player_name"),
            players.position,
            games.date,
            games.season,
            clubs.name.alias("club_name"),
            apps.minutes_played,
            apps.goals,
            apps.assists,
            apps.yellow_cards,
            apps.red_cards
        )
    )
    return fact

def write_gold(df, gold_path):
    df.write.mode("overwrite").parquet(f"{gold_path}/fact_player_match")

def verify(df, min_rows=1000):
   row_count = df.count()
   print(f"[VERIFY] fact_player_match row count = {row_count}")

   if row_count < min_rows:
       raise RuntimeError(
           f"Gold validation failed: row count {row_count} below threshold {min_rows}"
       )

   return row_count

def parse_args():
    parser = argparse.ArgumentParser(description="Build Gold fact_player_match")

    parser.add_argument(
        "--silver-path",
        required=True,
        help="Root path of silver tables"
    )

    parser.add_argument(
        "--gold-path",
        required=True,
        help="Root path for gold output"
    )

    return parser.parse_args()

def main():
    args = parse_args()

    silver_path = args.silver_path
    gold_path = args.gold_path

    print(f"[START] Silver path: {silver_path}")
    print(f"[START] Gold path: {gold_path}")

    spark = get_spark()
    players, games, apps, clubs = load_silver(spark, silver_path)
    fact = build_fact(players, games, clubs, apps)
    write_gold(fact, gold_path)
    row_count = verify(fact)

    print(f"[SUCCESS] Gold fact_player_match written with {row_count} rows")

    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e: 
        print(f"[FAILED] {str(e)}") 
        sys.exit(1)
