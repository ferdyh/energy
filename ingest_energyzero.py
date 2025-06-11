from prefect import flow, task
from datetime import datetime, timedelta

import duckdb
import requests

@flow(log_prints=True, name="Ingest EnergyZero Data")
def ingest_energyzero():
    """
    Ingests data from the EnergyZero dataset.
    """

    init_table()
    read_energyzero_data()
    write_to_duckdb()

@task(log_prints=True)
def read_energyzero_data():
    """
    Reads data from the EnergyZero dataset.
    """

    from_time = datetime.now().replace(second=0, microsecond=0, minute=0)
    to_time = datetime.now().replace(second=0, microsecond=0, minute=0) + timedelta(hours=33)

    url = f"https://api.energyzero.nl/v1/energyprices?fromDate={from_time.isoformat()}Z&tillDate={to_time.isoformat()}Z&interval=4&usageType=1&inclBtw=true"
    with open("/data/temp/energyzero.json", mode="w+") as f:
        f.write(requests.get(url).text)

@task(log_prints=True)
def init_table():
    """
    Initializes the DuckDB table for storing energy prices.
    """

    c = duckdb.connect(database='/data/db/energyzero.db')
    c.execute("""
        create table if not exists power_prices (
            reading_date timestamp primary key,
            price double
        );
    """)
    c.close()

@task(log_prints=True)
def write_to_duckdb():
    """
    Writes the ingested data to DuckDB.
    """

    c = duckdb.connect(database='/data/db/energyzero.db')
    c.execute("""
        insert or ignore into power_prices
        with data as (
            select unnest(prices) as price
            from read_json("/data/temp/energyzero.json")
        )
        select
            price.readingDate as reading_date,
            price.price
        from data;
    """)
    c.close()

if __name__ == "__main__":
    ingest_energyzero()
