
# ==============================================================
# COGNISTREAM
# Developer Flow-State & Cognitive Load Analytics
# Python + MySQL + SQLAlchemy
# ==============================================================

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ==============================================================
# 1. PROJECT CONFIGURATION
# ==============================================================

BASE_DIR = (
    Path.home()
    / "Desktop"
    / "project"
    / "CogniStream-Developer-Flow-State-Cognitive-Load-Analytics"
)

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# MYSQL CREDENTIALS

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Jerry0712@"
MYSQL_DATABASE = "cognistream_db"

print("\n" + "=" * 65)
print("       COGNISTREAM ANALYTICS PIPELINE")
print("=" * 65)


# ==============================================================
# 2. LOAD DATASETS
# ==============================================================

file_paths = {
    "IDE": DATA_DIR / "ide_events.csv",
    "Slack": DATA_DIR / "communication_events.csv",
    "Jira": DATA_DIR / "work_events.csv",
    "GitHub": DATA_DIR / "github_events.csv"
}

data = {}

print("\n[1] LOADING DATASETS")

for source, path in file_paths.items():

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    required = ["event_id", "developer_id", "timestamp"]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"{source} missing required columns: {missing}"
        )

    data[source] = df

    print(f"{source}: {len(df)} records loaded")


# ==============================================================
# 3. DATA QUALITY CHECK
# ==============================================================

print("\n[2] DATA QUALITY ANALYSIS")

quality_report = []

for source, df in data.items():

    quality_report.append({
        "source": source,
        "total_records": len(df),
        "duplicate_event_ids": df["event_id"].duplicated().sum(),
        "missing_developer_ids": df["developer_id"].isna().sum(),
        "missing_timestamps": df["timestamp"].isna().sum(),
        "total_missing_values": df.isnull().sum().sum()
    })

quality_df = pd.DataFrame(quality_report)

print(quality_df.to_string(index=False))


# ==============================================================
# 4. DATA CLEANING
# ==============================================================

print("\n[3] CLEANING DATASETS")

cleaned_data = {}

for source, df in data.items():

    df = df.copy()

    df = df.drop_duplicates(
        subset=["event_id"],
        keep="first"
    )

    for col in df.select_dtypes(include="object").columns:

        df[col] = df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    df = df.dropna(
        subset=["event_id", "developer_id", "timestamp"]
    )

    df["developer_id"] = (
        df["developer_id"].astype(str).str.upper()
    )

    if "session_id" in df.columns:

        df["session_id"] = (
            df["session_id"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    if "is_interruptive" in df.columns:

        df["is_interruptive"] = (
            df["is_interruptive"]
            .astype("string")
            .str.lower()
            .map({
                "true": True,
                "false": False,
                "1": True,
                "0": False,
                "yes": True,
                "no": False
            })
            .astype("boolean")
        )

    for col in ["additions", "deletions"]:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            ).fillna(0).astype(int)

    df = df.sort_values(
        ["developer_id", "timestamp"]
    ).reset_index(drop=True)

    cleaned_data[source] = df

    print(f"{source}: {len(df)} clean records")


# ==============================================================
# 5. UNIFIED ACTIVITY DATASET
# ==============================================================

print("\n[4] INTEGRATING ALL PLATFORMS")

standard_columns = [
    "event_id",
    "developer_id",
    "session_id",
    "timestamp",
    "source",
    "event_type",
    "project_name",
    "task_id",
    "task_status",
    "priority",
    "channel",
    "platform",
    "is_interruptive",
    "repository",
    "additions",
    "deletions",
    "app_name"
]

source_names = {
    "IDE": "VS Code",
    "Slack": "Slack",
    "Jira": "Jira",
    "GitHub": "GitHub"
}

unified_records = []

for source, df in cleaned_data.items():

    temp = df.copy()
    temp["source"] = source_names[source]

    for col in standard_columns:

        if col not in temp.columns:
            temp[col] = pd.NA

    unified_records.append(
        temp[standard_columns]
    )

activity_df = pd.concat(
    unified_records,
    ignore_index=True
)

activity_df = activity_df.sort_values(
    ["developer_id", "timestamp"]
).reset_index(drop=True)

activity_df.insert(
    0,
    "activity_id",
    range(1, len(activity_df) + 1)
)

activity_df["activity_date"] = (
    activity_df["timestamp"].dt.strftime("%Y-%m-%d")
)

activity_df["activity_hour"] = (
    activity_df["timestamp"].dt.hour
)

activity_df["day_name"] = (
    activity_df["timestamp"].dt.day_name()
)

print("Total integrated events:", len(activity_df))


# ==============================================================
# 6. FLOW-STATE / SESSION ANALYSIS
# ==============================================================

print("\n[5] FLOW-STATE ANALYSIS")

ide_df = cleaned_data["IDE"].copy()

if "session_id" not in ide_df.columns:
    raise ValueError("IDE dataset must contain session_id.")

ide_df["event_type"] = (
    ide_df["event_type"]
    .astype("string")
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("-", "_")
)

session_records = []

for (developer, session), group in ide_df.groupby(
    ["developer_id", "session_id"]
):

    if pd.isna(session):
        continue

    start_events = group[
        group["event_type"] == "session_start"
    ]

    end_events = group[
        group["event_type"] == "session_end"
    ]

    if start_events.empty or end_events.empty:
        continue

    session_start = start_events["timestamp"].min()
    session_end = end_events["timestamp"].max()

    duration = (
        session_end - session_start
    ).total_seconds() / 60

    if duration <= 0:
        continue

    project = (
        group["project_name"].dropna().iloc[0]
        if "project_name" in group.columns
        and group["project_name"].notna().any()
        else "Unknown"
    )

    coding_events = group[
        group["event_type"].isin([
            "coding_activity",
            "file_activity",
            "debugging"
        ])
    ]

    session_records.append({
        "developer_id": developer,
        "session_id": str(session),
        "project_name": project,
        "session_start": session_start,
        "session_end": session_end,
        "session_date": session_start.date(),
        "start_hour": session_start.hour,
        "duration_minutes": round(duration, 2),
        "coding_event_count": len(coding_events)
    })

session_df = pd.DataFrame(session_records)

if session_df.empty:
    raise ValueError(
        "No valid IDE sessions found. "
        "Check session_start and session_end event types."
    )

print("Valid coding sessions:", len(session_df))


# ==============================================================
# 7. INTERRUPTION ANALYSIS
# ==============================================================

print("\n[6] INTERRUPTION ANALYSIS")

slack_df = cleaned_data["Slack"].copy()

if "session_id" not in slack_df.columns:
    raise ValueError("Slack dataset must contain session_id.")

if "is_interruptive" not in slack_df.columns:
    raise ValueError("Slack dataset must contain is_interruptive.")

slack_df["is_interruptive"] = (
    slack_df["is_interruptive"].fillna(False).astype(bool)
)

slack_summary = (
    slack_df.groupby(
        ["developer_id", "session_id"],
        as_index=False
    )
    .agg(
        total_notifications=("event_id", "count"),
        interruptive_notifications=(
            "is_interruptive",
            "sum"
        )
    )
)

session_df = session_df.merge(
    slack_summary,
    on=["developer_id", "session_id"],
    how="left"
)

session_df["total_notifications"] = (
    session_df["total_notifications"].fillna(0).astype(int)
)

session_df["interruptive_notifications"] = (
    session_df["interruptive_notifications"].fillna(0).astype(int)
)

session_df["has_interruptions"] = (
    session_df["interruptive_notifications"] > 0
)

print(
    "Total Slack notifications:", len(slack_df)
)

print(
    "Interruptive notifications:",
    int(slack_df["is_interruptive"].sum())
)


# ==============================================================
# 8. FOCUS AND DEEP WORK CLASSIFICATION
# ==============================================================

print("\n[7] FOCUS ANALYSIS")

session_df["sustained_focus_proxy"] = (
    (session_df["duration_minutes"] >= 60)
    & (~session_df["has_interruptions"])
)

session_df["deep_work"] = (
    (session_df["duration_minutes"] >= 90)
    & (~session_df["has_interruptions"])
)


def classify_session(row):

    if row["deep_work"]:
        return "Deep Work"

    elif row["sustained_focus_proxy"]:
        return "Sustained Focus"

    elif row["has_interruptions"]:
        return "Interrupted Session"

    return "Short Session"


session_df["focus_category"] = (
    session_df.apply(classify_session, axis=1)
)


# ==============================================================
# 9. CONTEXT-SWITCHING ANALYSIS
# ==============================================================

print("\n[8] CONTEXT-SWITCHING ANALYSIS")

session_df = session_df.sort_values(
    ["developer_id", "session_start"]
).reset_index(drop=True)

session_df["previous_project"] = (
    session_df.groupby("developer_id")["project_name"].shift(1)
)

session_df["previous_session_end"] = (
    session_df.groupby("developer_id")["session_end"].shift(1)
)

session_df["gap_minutes"] = (
    session_df["session_start"]
    - session_df["previous_session_end"]
).dt.total_seconds() / 60

session_df["context_switch"] = (
    session_df["previous_project"].notna()
    & (
        session_df["project_name"]
        != session_df["previous_project"]
    )
)

session_df["gap_minutes"] = (
    session_df["gap_minutes"].round(2)
)

session_df["session_hours"] = (
    session_df["duration_minutes"] / 60
).round(2)

session_df["interruption_rate"] = (
    session_df["interruptive_notifications"]
    / session_df["total_notifications"].replace(0, pd.NA)
    * 100
).fillna(0).round(2)

print(
    "Project context switches:",
    int(session_df["context_switch"].sum())
)


# ==============================================================
# 10. SAVE ONLY TWO PROCESSED CSV FILES
# ==============================================================

print("\n[9] SAVING PROCESSED DATA")

activity_df.to_csv(
    OUTPUT_DIR / "activity_events.csv",
    index=False
)

session_df.to_csv(
    OUTPUT_DIR / "session_analytics.csv",
    index=False
)

print("Saved: activity_events.csv")
print("Saved: session_analytics.csv")


# ==============================================================
# 11. REMOVE OLD UNNECESSARY OUTPUT FILES
# ==============================================================

old_outputs = [
    "ide_cleaned.csv",
    "slack_cleaned.csv",
    "jira_cleaned.csv",
    "github_cleaned.csv",
    "developer_summary.csv",
    "source_summary.csv",
    "data_quality_report.csv",
    "hourly_productivity.csv",
    "daily_productivity.csv",
    "project_analytics.csv",
    "developer_productivity.csv",
    "interruption_analytics.csv"
]

for filename in old_outputs:

    old_file = OUTPUT_DIR / filename

    if old_file.exists():
        old_file.unlink()


# ==============================================================
# 12. CONNECT TO MYSQL AND CREATE DATABASE
# ==============================================================

print("\n[10] CONNECTING TO MYSQL")

server_url = URL.create(
    drivername="mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT
)

server_engine = create_engine(
    server_url,
    pool_pre_ping=True
)

try:

    with server_engine.begin() as connection:

        connection.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                "CHARACTER SET utf8mb4 "
                "COLLATE utf8mb4_unicode_ci"
            )
        )

    print("Database ready:", MYSQL_DATABASE)

except Exception as error:

    raise RuntimeError(
        "MySQL connection failed. Check MySQL Server "
        "and your login credentials."
    ) from error

finally:
    server_engine.dispose()


# ==============================================================
# 13. CREATE DATABASE ENGINE
# ==============================================================

database_url = URL.create(
    drivername="mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE
)

engine = create_engine(
    database_url,
    pool_pre_ping=True
)


# ==============================================================
# 14. LOAD DATA INTO MYSQL
# ==============================================================

print("\n[11] LOADING DATA INTO MYSQL")

try:

    activity_df.to_sql(
        name="activity_events",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    print(
        "activity_events loaded:",
        len(activity_df),
        "records"
    )

    session_df.to_sql(
        name="session_analytics",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    print(
        "session_analytics loaded:",
        len(session_df),
        "records"
    )

except Exception as error:

    raise RuntimeError(
        "Failed to load data into MySQL."
    ) from error


# ==============================================================
# 15. VERIFY MYSQL DATA
# ==============================================================

print("\n[12] VERIFYING MYSQL TABLES")

with engine.connect() as connection:

    activity_count = connection.execute(
        text("SELECT COUNT(*) FROM activity_events")
    ).scalar_one()

    session_count = connection.execute(
        text("SELECT COUNT(*) FROM session_analytics")
    ).scalar_one()

    print("activity_events:", activity_count, "records")
    print("session_analytics:", session_count, "records")

engine.dispose()


# ==============================================================
# 16. FINAL EXECUTION REPORT
# ==============================================================

print("\n" + "=" * 65)
print("             COGNISTREAM FINAL REPORT")
print("=" * 65)

print("\nTotal developers:",
      activity_df["developer_id"].nunique())

print("Total integrated events:",
      len(activity_df))

print("Total valid sessions:",
      len(session_df))

print(
    "Total session hours:",
    round(session_df["duration_minutes"].sum() / 60, 2)
)

print(
    "Total interruptions:",
    int(session_df["interruptive_notifications"].sum())
)

print(
    "Deep work sessions:",
    int(session_df["deep_work"].sum())
)

print(
    "Context switches:",
    int(session_df["context_switch"].sum())
)

print("\nProcessed CSV files:")
print("1. activity_events.csv")
print("2. session_analytics.csv")

print("\nMySQL database:", MYSQL_DATABASE)
print("Tables:")
print("1. activity_events")
print("2. session_analytics")

print("\n" + "=" * 65)
print("       COGNISTREAM PIPELINE COMPLETED")
print("=" * 65)
