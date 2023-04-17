# from dotenv import load_dotenv
import os
import sys
import time

import pandas as pd
from pydantic import BaseSettings
from sqlalchemy import create_engine


class Settings(BaseSettings):
    MY_SQL_USER: str
    MY_SQL_PASSWORD: str
    MY_SQL_HOST: str
    MY_SQL_PORT: int
    MY_SQL_DATABASE: str
    MY_SQL_TABLE_NAME_VOTES: str
    MY_SQL_TABLE_NAME_MANDATES: str
    MY_SQL_TABLE_NAME_POLLS: str

    class Config:
        env_file = ".env"


def main():
    settings = Settings()

    user = settings.MY_SQL_USER
    password = settings.MY_SQL_PASSWORD
    host = settings.MY_SQL_HOST
    port = settings.MY_SQL_PORT
    db = settings.MY_SQL_DATABASE
    table_name_votes = settings.MY_SQL_TABLE_NAME_VOTES
    table_name_mandates = settings.MY_SQL_TABLE_NAME_MANDATES
    table_name_polls = settings.MY_SQL_TABLE_NAME_POLLS

    time.sleep(15)

    print(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    sys.stdout.flush()

    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    )
    pd.io.parquet.get_engine("auto")

    print("Reading data...")
    sys.stdout.flush()
    df_all_votes = pd.read_parquet(
        "./data/abgeordnetenwatch/df_all_votes_111.parquet"
    )
    df_mandates = pd.read_parquet(
        "./data/abgeordnetenwatch/df_mandates_111.parquet"
    )
    df_polls = pd.read_parquet("./data/abgeordnetenwatch/df_polls_111.parquet")

    print("Inserting data...")
    sys.stdout.flush()
    df_all_votes.to_sql(name=table_name_votes, con=engine, if_exists="append")
    df_mandates.to_sql(
        name=table_name_mandates, con=engine, if_exists="append"
    )
    df_polls.to_sql(name=table_name_polls, con=engine, if_exists="append")

    print("Done!")


if __name__ == "__main__":
    main()
