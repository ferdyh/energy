import duckdb
import requests
from prefect import task, flow

database = "/data/db/forecast_solar.db"

@flow(log_prints=True, name="Ingest Forecast Solar Data")
def ingest_forecast_solar():
    """
    Ingests data from the Forecast Solar dataset.
    """

    init_tables()
    read_forecast_data()
    write_to_duckdb()

@task(log_prints=True)
def read_forecast_data():
    """
    Reads data from the EnergyZero dataset.
    """

    url = f"https://api.forecast.solar/estimate/53.0277/6.8421/45/20/3.86"
    with open("/data/temp/forecast_solar.json", mode="w+") as f:
        f.write(requests.get(url).text)

@task(log_prints=True)
def init_tables():
    """
    Initializes the DuckDB table for storing energy prices.
    """

    print("Initializing DuckDB table...")

    c = duckdb.connect(database=database)
    c.execute("""
        create table if not exists hourly (
            timestmap timestamp primary key,
            watts integer,
            watt_hours_period integer,
            watt_hours integer
        );
    """)

    c.execute("""
        create table if not exists daily (
            date date primary key,
            watt_hours_day integer
        );
    """)

    c.close()

@task(log_prints=True)
def write_to_duckdb():
    """
    Writes the ingested data to DuckDB.
    """

    c = duckdb.connect(database=database)
    c.execute("""
        insert or replace into hourly
        select
            strptime(unnest(json_keys(result.watts)), '%x %X') as timestamp,
            json_value(result.watts, strftime(timestamp, '%x %X'))::integer as watts,
            json_value(result.watt_hours_period, strftime(timestamp, '%x %X'))::integer as watt_hours_period,
            json_value(result.watt_hours, strftime(timestamp, '%x %X'))::integer as watt_hours
        from read_json("/data/temp/forecast_solar.json");
    """)
    c.execute("""
        insert or replace into daily
        select
            strptime(unnest(json_keys(result.watt_hours_day)), '%x')::DATE as date,
            json_value(result.watt_hours_day, strftime(date, '%x'))::integer as watt_hours_day,
        from read_json("/data/temp/forecast_solar.json");
    """)
    c.close()

if __name__ == "__main__":
    ingest_forecast_solar()
