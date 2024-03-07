import pandas as pd
import os
from dagster import asset

@asset
def pull_cases() -> None:
    """
    Get the historical data of COVID-19 cases and tests by country.
    """

    # read data from api
    url = "https://covid.ourworldindata.org/data/internal/megafile--cases-tests.json"
    df = pd.read_json(url)

    # rename the country column name
    df.rename(columns={"location":"country"}, inplace=True)

    # make sure that model folder is created
    os.makedirs("model", exist_ok=True)

    # write data to database or elsewhere
    df.to_parquet("model/f_daily_cases.parquet.gz", index=False)

@asset(deps=[pull_cases])
def generate_calendar() -> None:
    """
    Create a date calendar based on the f_daily_cases.
    """

    # read the fact table
    df = pd.read_parquet("model/f_daily_cases.parquet.gz")

    # get the unique dates
    df_date = df[["date"]].astype("datetime64[ns]").drop_duplicates()

    # add more attributes
    df_date["year"] = df["date"].dt.year
    df_date["month"] = df["date"].dt.month
    df_date["day"] = df["date"].dt.day

    # write data to database or elsewhere
    df_date.to_parquet("model/d_date.parquet.gz", index=False)

@asset(deps=[pull_cases])
def generate_countries() -> None:
    """
    Create a country table based on the f_daily_cases.
    """

    # make sure that model folder is created
    df = pd.read_parquet("model/f_daily_cases.parquet.gz")

    # get the unique countries
    df_country = df[["country"]].drop_duplicates()

    # write data to database or elsewhere
    df_country.to_parquet("model/d_country.parquet.gz", index=False)

@asset(deps=[generate_calendar, generate_countries])
def pull_deaths() -> None:
    """
    Get the historical data of COVID-19 deaths by country.
    """

    # read data from api
    url = "https://covid.ourworldindata.org/data/internal/megafile--deaths.json"
    df = pd.read_json(url)

    # drop continent column
    df.drop(columns=["continent"], errors="ignore", inplace=True)

    # rename the country column name
    df.rename(columns={"location":"country"}, inplace=True)

    # read dimension tables
    df_date = pd.read_parquet("model/d_date.parquet.gz")
    df_country = pd.read_parquet("model/d_country.parquet.gz")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date[["date"]])
        .merge(df_country[["country"]])
    )

    # write data to database or elsewhere
    df.to_parquet("model/f_daily_deaths.parquet.gz", index=False)

@asset(deps=[generate_calendar, generate_countries])
def pull_vaccinations() -> None:
    """
    Get the historical data of COVID-19 vaccinations by country.
    """

    # read data from api
    url = "https://covid.ourworldindata.org/data/internal/megafile--vaccinations.json"
    df = pd.read_json(url)

    # rename the country column name
    df.rename(columns={"location":"country"}, inplace=True)

    # read dimension tables
    df_date = pd.read_parquet("model/d_date.parquet.gz")
    df_country = pd.read_parquet("model/d_country.parquet.gz")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date[["date"]])
        .merge(df_country[["country"]])
    )

    # write data to database or elsewhere
    df.to_parquet("model/f_daily_vaccinations.parquet.gz", index=False)

@asset(deps=[generate_calendar, generate_countries])
def pull_hospital_admissions() -> None:
    """
    Get the historical data of COVID-19 hospital patients and admissions by country.
    """

    # read data from api
    url = "https://covid.ourworldindata.org/data/internal/megafile--hospital-admissions.json"
    df = pd.read_json(url)

    # rename the country column name
    df.rename(columns={"location":"country"}, inplace=True)

    # read dimension tables
    df_date = pd.read_parquet("model/d_date.parquet.gz")
    df_country = pd.read_parquet("model/d_country.parquet.gz")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date[["date"]])
        .merge(df_country[["country"]])
    )

    # write data to database or elsewhere
    df.to_parquet("model/f_daily_hospital_admissions.parquet.gz", index=False)

@asset(deps=[generate_calendar, generate_countries])
def pull_excess_mortality() -> None:
    """
    Get the historical data of COVID-19 excess mortality by country.
    """

    # read data from api
    url = "https://covid.ourworldindata.org/data/internal/megafile--excess-mortality.json"
    df = pd.read_json(url)

    # rename the country column name
    df.rename(columns={"location":"country"}, inplace=True)

    # read dimension tables
    df_date = pd.read_parquet("model/d_date.parquet.gz")
    df_country = pd.read_parquet("model/d_country.parquet.gz")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date[["date"]])
        .merge(df_country[["country"]])
    )

    # write data to database or elsewhere
    df.to_parquet("model/f_daily_excess_mortality.parquet.gz", index=False)