import psycopg2
import pandas as pd
import os

EXCEL_FILE = os.path.join(os.path.dirname(__file__), 'Interposer SXM4 7-14-25.xlsx')
DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def connect_to_db():
    return psycopg2.connect(**DB_CONFIG)

def main():
    print("\n--- Cosmetic Damage Serial Analysis ---\n")
    # 1. Read serials from Excel
    try:
        df_excel = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"❌ Failed to read Excel file: {e}")
        return
    # Try to find the serial column
    serial_col = None
    for col in df_excel.columns:
        if col.strip().lower() in ["sn", "serial", "serial_number", "serialnumber"]:
            serial_col = col
            break
    if not serial_col:
        print(f"❌ Could not find serial number column in Excel. Columns: {list(df_excel.columns)}")
        return
    serials = df_excel[serial_col].astype(str).str.strip().unique().tolist()
    print(f"Found {len(serials)} unique serials in Excel file.")

    # 2. Connect to DB
    try:
        conn = connect_to_db()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    cursor = conn.cursor()

    # 3. Query for damaged group
    format_strings = ','.join(['%s'] * len(serials))
    query_damaged = f"""
        SELECT sn, history_station_passing_status
        FROM testboard_master_log
        WHERE sn IN ({format_strings})
    """
    cursor.execute(query_damaged, serials)
    damaged_rows = cursor.fetchall()
    df_damaged = pd.DataFrame(damaged_rows, columns=["sn", "history_station_passing_status"])

    # 4. Query for others
    query_others = f"""
        SELECT sn, history_station_passing_status
        FROM testboard_master_log
        WHERE sn NOT IN ({format_strings})
    """
    cursor.execute(query_others, serials)
    others_rows = cursor.fetchall()
    df_others = pd.DataFrame(others_rows, columns=["sn", "history_station_passing_status"])

    # 5. Calculate fail rates
    def fail_rate(df):
        if df.empty:
            return 0, 0, 0.0
        failed = df["history_station_passing_status"].str.lower().eq("fail").sum()
        total = len(df)
        rate = failed / total if total else 0.0
        return failed, total, rate

    failed_d, total_d, rate_d = fail_rate(df_damaged)
    failed_o, total_o, rate_o = fail_rate(df_others)

    # 6. Find serials from Excel not in DB
    found_serials = set(df_damaged["sn"]) if not df_damaged.empty else set()
    not_found = sorted(set(serials) - found_serials)

    # 7. Print summary
    print(f"Damaged group (from Excel):")
    print(f"  Total serials in DB: {total_d} / {len(serials)}")
    print(f"  Failed: {failed_d}")
    print(f"  Fail rate: {rate_d:.2%}")
    print()
    print(f"Other group (all other serials):")
    print(f"  Total serials: {total_o}")
    print(f"  Failed: {failed_o}")
    print(f"  Fail rate: {rate_o:.2%}")
    print()
    if not_found:
        print("Serials from Excel not found in DB:")
        print(", ".join(not_found))
    else:
        print("All serials from Excel were found in the DB.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main() 