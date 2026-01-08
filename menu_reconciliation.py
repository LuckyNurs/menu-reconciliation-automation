import pandas as pd
import mysql.connector
import psycopg2
import os

# =========================
# DATABASE CONFIG (SAFE)
# =========================
DB_CONFIG_SOURCE = {
    "host": os.getenv("SRC_DB_HOST", "db-source.example.com"),
    "user": os.getenv("SRC_DB_USER", "readonly_user"),
    "password": os.getenv("SRC_DB_PASSWORD", "password"),
    "database": os.getenv("SRC_DB_NAME", "source_db"),
}

DB_CONFIG_TARGET = {
    "host": os.getenv("TGT_DB_HOST", "db-target.example.com"),
    "user": os.getenv("TGT_DB_USER", "readonly_user"),
    "password": os.getenv("TGT_DB_PASSWORD", "password"),
    "database": os.getenv("TGT_DB_NAME", "target_db"),
    "port": os.getenv("TGT_DB_PORT", "5432"),
}

# =========================
# SAMPLE OUTLETS
# =========================
OUTLET_CODES = [
    "OUTLET_01",
    "OUTLET_02",
    "OUTLET_03",
]

# =========================
# CONNECTION HELPERS
# =========================
def get_mysql_connection():
    return mysql.connector.connect(**DB_CONFIG_SOURCE)

def get_postgres_connection():
    return psycopg2.connect(**DB_CONFIG_TARGET)

# =========================
# FETCH SOURCE DATA
# =========================
def fetch_source_data(outlet_code):
    conn = get_mysql_connection()
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT
        menu_id,
        menu_name,
        is_active
    FROM source_menu_table
    WHERE is_active = 1
      AND outlet_code = %s;
    """

    cursor.execute(query, (outlet_code,))
    rows = cursor.fetchall()
    conn.close()

    return pd.DataFrame(rows, columns=["menu_id", "menu_name", "is_active"])

# =========================
# FETCH TARGET DATA
# =========================
def fetch_target_data(outlet_code):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT
        menu_item_id AS menu_id,
        menu_item_name AS menu_name
    FROM target_menu_table
    WHERE is_active = true
      AND outlet_code = %s;
    """

    cursor.execute(query, (outlet_code,))
    rows = cursor.fetchall()
    conn.close()

    return pd.DataFrame(rows, columns=["menu_id", "menu_name"])

# =========================
# MOCK ALERTING
# =========================
def send_alert(message):
    print("\n=== MOCK ALERT ===")
    print(message)
    print("==================\n")

# =========================
# MAIN RECONCILIATION
# =========================
summary_message = "📊 MENU RECONCILIATION SUMMARY\n"

for outlet in OUTLET_CODES:
    print(f"\n🔍 Checking outlet: {outlet}")

    src_df = fetch_source_data(outlet)
    tgt_df = fetch_target_data(outlet)

    src_df = src_df.rename(columns={"menu_name": "source_menu_name"})
    tgt_df = tgt_df.rename(columns={"menu_name": "target_menu_name"})

    src_df["menu_id"] = src_df["menu_id"].astype(str).str.strip().str.lower()
    tgt_df["menu_id"] = tgt_df["menu_id"].astype(str).str.strip().str.lower()

    merged = pd.merge(
        src_df,
        tgt_df,
        on="menu_id",
        how="outer",
        indicator=True
    )

    merged["_merge"] = merged["_merge"].replace({
        "left_only": "source_only",
        "right_only": "target_only",
        "both": "matched"
    })

    merged.to_csv(f"menu_recon_{outlet}.csv", index=False)

    count_source_only = (merged["_merge"] == "source_only").sum()
    count_target_only = (merged["_merge"] == "target_only").sum()

    summary_message += (
        f"\n📍 {outlet}\n"
        f"- Source only: {count_source_only}\n"
        f"- Target only: {count_target_only}\n"
    )

# =========================
# FINAL REPORT
# =========================
send_alert(summary_message)
