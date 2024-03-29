import pandas as pd
import os

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from urllib.parse import quote_plus
from dagster import asset

DRIVER = os.getenv("DRIVER")
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

connection_string = (
    f'Driver={DRIVER};'
    f'SERVER={SERVER};'
    f'Database={DATABASE};'
    f'UID={USER};'
    f'PWD={PASSWORD};'
    'Trusted_Connection=no;'
)

connection_uri = f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"
engine = create_engine(connection_uri, fast_executemany=True)
conn = engine.connect()

def db_create_schema(schema_name:str):
    if schema_name not in conn.dialect.get_schema_names(conn):
        conn.execute(CreateSchema(schema_name))

def db_run_query(
        query_string:str,
        return_table = True
    ):
    """
    Read data from the database, and assign the output as a dataframe.
    """
    if return_table:
        df = pd.read_sql( 
            sql=query_string, 
            con=engine
        )
        return df

def write_data_to_db(
        df:pd.DataFrame, 
        table_name:str, 
        schema:str, 
        insert_type="replace", 
        chunks=None
    ):
    """
    Write data from a dataframe to the database.
    """
    df.to_sql(
        name=table_name, 
        schema=schema, 
        con=engine, 
        if_exists=insert_type, 
        chunksize=chunks, 
        index=False
    )
    return df

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

    # create a schema if not exists
    db_create_schema("fact")

    # write data to database
    write_data_to_db(df=df, table_name="daily_cases", schema="fact")

@asset(deps=[pull_cases])
def generate_calendar() -> None:
    """
    Create a date calendar based on the daily_cases.
    """

    # get the unique dates
    df = db_run_query("select distinct [date] from fact.daily_cases")

    # add more attributes
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    # create a schema if not exists
    db_create_schema("dim")

    # write data to database
    write_data_to_db(df=df, table_name="calendar", schema="dim")

@asset(deps=[pull_cases])
def generate_countries() -> None:
    """
    Create a country table based on the daily_cases.
    """

    # get the unique countries
    df = db_run_query("select distinct [country] from fact.daily_cases")

    # create a schema if not exists
    db_create_schema("dim")

    # write data to database
    write_data_to_db(df=df, table_name="country", schema="dim")

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
    df_date = db_run_query("select distinct [date] from dim.calendar")
    df_country = db_run_query("select distinct [country] from dim.country")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date)
        .merge(df_country)
    )

    # write data to database
    write_data_to_db(df=df, table_name="daily_deaths", schema="fact")

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
    df_date = db_run_query("select distinct [date] from dim.calendar")
    df_country = db_run_query("select distinct [country] from dim.country")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date)
        .merge(df_country)
    )

    # write data to database
    write_data_to_db(df=df, table_name="daily_vaccinations", schema="fact")

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
    df_date = db_run_query("select distinct [date] from dim.calendar")
    df_country = db_run_query("select distinct [country] from dim.country")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date)
        .merge(df_country)
    )

    # write data to database
    write_data_to_db(df=df, table_name="daily_hospital_admissions", schema="fact")

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
    df_date = db_run_query("select distinct [date] from dim.calendar")
    df_country = db_run_query("select distinct [country] from dim.country")

    # inner join to make sure foreign keys
    df = (
        df
        .merge(df_date)
        .merge(df_country)
    )

    # write data to database
    write_data_to_db(df=df, table_name="daily_excess_mortality", schema="fact")