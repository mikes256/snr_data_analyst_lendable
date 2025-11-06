import duckdb
from pathlib import Path

DB_PATH = "/workspaces/snr_data_analyst_lendable/lendable/dev.duckdb"
OUT_DIR = Path("/workspaces/snr_data_analyst_lendable/lendable")

models = {
    "int_intercom__daily_base": """
        SELECT CAST(calendar_date AS DATE) AS calendar_date, *
        FROM main.int_intercom__daily_base
    """,
    "int_intercom__source_summary": """
        SELECT CAST(calendar_date AS DATE) AS calendar_date, *
        FROM main.int_intercom__source_summary
    """,
    "int_intercom__ai_v_human_metrics": """
        SELECT CAST(calendar_date AS DATE) AS calendar_date, *
        FROM main.int_intercom__ai_v_human_metrics
    """,
    "int_intercom__calendar": """
        SELECT CAST(calendar_date AS DATE) AS calendar_date
        FROM main.int_intercom__calendar
    """,
}

con = duckdb.connect(DB_PATH)
con.execute("SET schema 'main'")

for name, sql in models.items():
    out = OUT_DIR / f"{name}.csv"
    con.execute(f"""
        COPY ({sql})
        TO '{out.as_posix()}'
        WITH (HEADER, DELIMITER ',');
    """)
    print(f"✅ Exported {name} -> {out}")
