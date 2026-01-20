import datetime as dt
import hashlib
import os
import sys
from pathlib import Path

import pandas as pd

app_dir = Path(__file__).resolve().parents[1] / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

from queries import (  # noqa: E402
    get_disaster_date_bounds,
    get_distinct_disaster_types,
    get_sankey_rows,
    upsert_name_grouping_cache,
)
from llm import group_sankey_names  # noqa: E402

YEAR_START = 1953
CHUNK_SIZE = 50
TIMEOUT_S = 60


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _print_status(message: str) -> None:
    print(message, flush=True)


def _build_records(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["record_id"] = df["record_id"].astype(str)
    df["declaration_name"] = df["declaration_name"].fillna("").astype(str).str.strip()
    df["disaster_declaration_date"] = pd.to_datetime(df["disaster_declaration_date"])
    df["disaster_begin_date"] = pd.to_datetime(df.get("disaster_begin_date"), errors="coerce")
    df["disaster_end_date"] = pd.to_datetime(df.get("disaster_end_date"), errors="coerce")
    effective_date = (
        df["disaster_declaration_date"]
        .combine_first(df["disaster_begin_date"])
        .combine_first(df["disaster_end_date"])
    )
    df["year"] = effective_date.dt.year.astype(int).astype(str)
    df["source_text_hash"] = (
        df["disaster_type"].astype(str) + "|" + df["declaration_name"]
    ).map(_hash_text)
    return df[
        ["record_id", "year", "disaster_type", "declaration_name", "source_text_hash"]
    ]


def main() -> None:
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    type_df = get_distinct_disaster_types().df
    type_options = (
        type_df["disaster_type"].dropna().astype(str).tolist()
        if not type_df.empty
        else []
    )
    bounds_df = get_disaster_date_bounds().df
    if not bounds_df.empty:
        min_date = bounds_df.at[0, "min_date"]
        year_start = int(pd.to_datetime(min_date).year)
    else:
        year_start = YEAR_START
    year_end = dt.date.today().year
    _print_status(
        f"Pre-warm starting ({year_end}->{year_start}), chunk={CHUNK_SIZE}, model={model}"
    )
    for year in range(year_end, year_start - 1, -1):
        year_rows = 0
        for disaster_type in type_options:
            start = dt.date(year, 1, 1).isoformat()
            end = dt.date(year + 1, 1, 1).isoformat()
            result = get_sankey_rows(start, end, [disaster_type])
            df = result.df
            if df.empty:
                continue

            records = _build_records(df).drop_duplicates(subset=["record_id"])
            total_records = int(records.shape[0])
            total_batches = max((total_records + CHUNK_SIZE - 1) // CHUNK_SIZE, 1)
            _print_status(
                f"{year}/{disaster_type}: {total_records} records ({total_batches} batches)"
            )

            llm_rows: list[dict] = []
            for idx in range(0, total_records, CHUNK_SIZE):
                batch = records.iloc[idx : idx + CHUNK_SIZE]
                _print_status(
                    f"OpenAI request: {year}/{disaster_type} "
                    f"batch {idx // CHUNK_SIZE + 1}/{total_batches} "
                    f"records {len(batch)}"
                )
                batch_rows = group_sankey_names(
                    batch[["record_id", "year", "disaster_type", "declaration_name"]].to_dict(
                        "records"
                    ),
                    timeout_s=TIMEOUT_S,
                    chunk_size=CHUNK_SIZE,
                )
                if batch_rows:
                    hash_map = dict(zip(batch["record_id"], batch["source_text_hash"]))
                    for row in batch_rows:
                        record_id = str(row.get("record_id"))
                        row["record_id"] = record_id
                        row["source_text_hash"] = hash_map.get(record_id, "")
                        row["llm_model"] = model
                    llm_rows.extend(batch_rows)
                completed = min(idx + CHUNK_SIZE, total_records)
                _print_status(
                    f"LLM {year}/{disaster_type}: batch {idx // CHUNK_SIZE + 1}/{total_batches} "
                    f"({completed}/{total_records})"
                )

            if llm_rows:
                upsert_name_grouping_cache(llm_rows)
                _print_status(f"LLM {year}/{disaster_type}: upserted {len(llm_rows)} rows")
                year_rows += len(llm_rows)
            else:
                _print_status(f"LLM {year}/{disaster_type}: no rows to upsert")
        if year_rows == 0:
            _print_status(f"{year}: no records across types")


if __name__ == "__main__":
    main()
