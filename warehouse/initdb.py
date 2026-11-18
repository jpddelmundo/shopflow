import duckdb

from pathlib import Path

duckdb_conn = duckdb.connect("staging_db.duckdb")

tables = ["staging", "warehouse"]

def main():
    for file in tables:
        with open(f"warehouse/tables/{file}.sql", "r") as sql:
            duckdb_conn.execute(sql.read())

if __name__ == "__main__":
    main()