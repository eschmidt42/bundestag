import sys
import time

import polars as pl
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MY_SQL_USER: str = Field(default=...)
    MY_SQL_PASSWORD: str = Field(default=...)
    MY_SQL_HOST: str = Field(default=...)
    MY_SQL_PORT: int = Field(default=...)
    MY_SQL_DATABASE: str = Field(default=...)
    MY_SQL_TABLE_NAME_VOTES: str = Field(default=...)
    MY_SQL_TABLE_NAME_MANDATES: str = Field(default=...)
    MY_SQL_TABLE_NAME_POLLS: str = Field(default=...)

    model_config = SettingsConfigDict(env_file=".env")


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

    uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

    print("Reading data...")
    sys.stdout.flush()
    df_all_votes = pl.read_parquet("./data/abgeordnetenwatch/votes_111.parquet")
    df_mandates = pl.read_parquet("./data/abgeordnetenwatch/mandates_111.parquet")
    df_polls = pl.read_parquet("./data/abgeordnetenwatch/polls_111.parquet")

    print("Inserting data...")
    sys.stdout.flush()
    df_all_votes.write_database(
        table_name=table_name_votes, connection=uri, if_table_exists="append"
    )
    df_mandates.write_database(
        table_name=table_name_mandates, connection=uri, if_table_exists="append"
    )
    df_polls.write_database(
        table_name=table_name_polls, connection=uri, if_table_exists="append"
    )

    print("Done!")


if __name__ == "__main__":
    main()
