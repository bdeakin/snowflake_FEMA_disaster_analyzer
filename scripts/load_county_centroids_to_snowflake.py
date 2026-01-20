import csv
import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import OperationalError


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _flag_enabled(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def main() -> None:
    load_dotenv()
    csv_path = Path("data/county_centroids.csv").resolve()
    if not csv_path.exists():
        raise FileNotFoundError("data/county_centroids.csv not found. Run download script first.")

    ocsp_fail_open = _flag_enabled("SNOWFLAKE_OCSP_FAIL_OPEN")
    disable_ocsp_checks = _flag_enabled("SNOWFLAKE_DISABLE_OCSP_CHECKS")
    conn = snowflake.connector.connect(
        account=get_env("SNOWFLAKE_ACCOUNT"),
        user=get_env("SNOWFLAKE_USER"),
        password=get_env("SNOWFLAKE_PASSWORD"),
        role=get_env("SNOWFLAKE_ROLE"),
        warehouse=get_env("SNOWFLAKE_WAREHOUSE"),
        database="ANALYTICS",
        schema="REF",
        ocsp_fail_open=ocsp_fail_open,
        disable_ocsp_checks=disable_ocsp_checks,
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS ANALYTICS.REF.COUNTY_CENTROIDS ("
            "county_fips STRING,"
            "county_name STRING,"
            "state_fips STRING,"
            "state_abbr STRING,"
            "centroid_lat FLOAT,"
            "centroid_lon FLOAT"
            ");"
        )
        cur.execute("TRUNCATE TABLE ANALYTICS.REF.COUNTY_CENTROIDS;")
        try:
        cur.execute("CREATE OR REPLACE TEMPORARY STAGE county_centroids_stage;")
        cur.execute(
                f"PUT 'file://{csv_path.as_posix()}' @county_centroids_stage AUTO_COMPRESS=TRUE;"
        )
        cur.execute(
            "COPY INTO ANALYTICS.REF.COUNTY_CENTROIDS "
            "FROM @county_centroids_stage "
                "FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
            "PURGE=TRUE;"
        )
        except OperationalError:
            rows = []
            with csv_path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            cur.executemany(
                "INSERT INTO ANALYTICS.REF.COUNTY_CENTROIDS "
                "(county_fips, county_name, state_fips, state_abbr, centroid_lat, centroid_lon) "
                "VALUES (%(county_fips)s, %(county_name)s, %(state_fips)s, %(state_abbr)s, "
                "%(centroid_lat)s, %(centroid_lon)s)",
                rows,
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
