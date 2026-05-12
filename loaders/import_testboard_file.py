import sys
import os
import math
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE

UNIQUE_KEY_FIELDS = (
    'sn',
    'pn',
    'model',
    'work_station_process',
    'baseboard_sn',
    'baseboard_pn',
    'workstation_name',
    'history_station_start_time',
    'history_station_end_time',
    'history_station_passing_status',
    'operator',
    'failure_reasons',
    'failure_note',
    'failure_code',
    'diag_version',
    'fixture_no',
    'data_source',
)


def connect_to_db():
    return psycopg2.connect(**DATABASE)


def clean_column_name(col_name):
    return col_name.lower().replace(' ', '_').replace('-', '_')


def normalize_string(value):
    if pd.isna(value):
        return "nan"
    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return "nan"
    return text


def normalize_datetime(value):
    if pd.isna(value):
        return None
    return pd.to_datetime(value).to_pydatetime()


def normalize_int(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(value)
    except Exception:
        return None


def build_unique_key(row):
    return tuple(row[field] for field in UNIQUE_KEY_FIELDS)


def main():
    if len(sys.argv) != 2:
        print("Usage: python import_testboard_file.py /path/to/file.xlsx", file=sys.stderr)
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Importing {file_path} into testboard_master_log...")
    conn = connect_to_db()

    try:
        df = pd.read_excel(file_path)
        df.columns = [clean_column_name(col) for col in df.columns]
        df['data_source'] = 'testboard'

        # Initial spreadsheet-level cleanup.
        dedup_cols = [c for c in df.columns if c != 'number_of_times_baseboard_is_used']
        original_count = len(df)
        df = df.drop_duplicates(subset=dedup_cols)
        cleaned_count = len(df)

        if original_count != cleaned_count:
            print(
                f"Cleaned {original_count - cleaned_count:,} duplicate rows "
                f"(ignoring 'number_of_times_baseboard_is_used' column)"
            )
            print(f"Original rows: {original_count:,}, Cleaned rows: {cleaned_count:,}")

        mapped_data = []
        for _, row in df.iterrows():
            mapped_row = {
                'sn': normalize_string(row.get('product_sn')),
                'pn': normalize_string(row.get('product_pn')),
                'model': normalize_string(row.get('model')),
                'work_station_process': normalize_string(row.get('work_station_process')),
                'baseboard_sn': normalize_string(row.get('baseboard_sn')),
                'baseboard_pn': normalize_string(row.get('baseboard_pn')),
                'workstation_name': normalize_string(row.get('workstation_name')),
                'history_station_start_time': normalize_datetime(row.get('history_station_start_time')),
                'history_station_end_time': normalize_datetime(row.get('history_station_end_time')),
                'history_station_passing_status': normalize_string(row.get('history_station_passing_status')),
                'operator': normalize_string(row.get('operator')),
                'failure_reasons': normalize_string(row.get('failure_reasons')),
                'failure_note': normalize_string(row.get('failure_note')),
                'failure_code': normalize_string(row.get('failure_code')),
                'diag_version': normalize_string(row.get('diag_version')),
                'fixture_no': normalize_string(row.get('fixture_no')),
                'data_source': 'testboard',
            }
            mapped_data.append(mapped_row)

        # Second-pass dedupe on the exact database unique key.
        unique_rows = []
        seen_keys = set()
        duplicate_rows_in_batch = 0
        for row in mapped_data:
            key = build_unique_key(row)
            if key in seen_keys:
                duplicate_rows_in_batch += 1
                continue
            seen_keys.add(key)
            unique_rows.append(row)

        if duplicate_rows_in_batch:
            print(f"Removed {duplicate_rows_in_batch:,} duplicate rows from the incoming batch after mapping")

        if not unique_rows:
            print("No rows left to import after cleanup")
            try:
                os.remove(file_path)
                print(f"Deleted XLSX file: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"Could not delete XLSX file: {e}")
                sys.exit(1)
            sys.exit(0)

        cursor = conn.cursor()

        print(f"About to upsert {len(unique_rows):,} deduplicated records...")
        for i, row in enumerate(unique_rows[:3]):
            print(f"Record {i + 1}:")
            for key, value in row.items():
                print(f"  {key}: {value} (type: {type(value)})")
            print()

        insert_query = """
        INSERT INTO testboard_master_log (
            sn, pn, model, work_station_process, baseboard_sn, baseboard_pn, workstation_name,
            history_station_start_time, history_station_end_time, history_station_passing_status, operator,
            failure_reasons, failure_note, failure_code, diag_version, fixture_no, data_source
        ) VALUES %s
        ON CONFLICT DO NOTHING
        """

        values = [(
            row['sn'], row['pn'], row['model'], row['work_station_process'], row['baseboard_sn'], row['baseboard_pn'], row['workstation_name'],
            row['history_station_start_time'], row['history_station_end_time'], row['history_station_passing_status'], row['operator'],
            row['failure_reasons'], row['failure_note'], row['failure_code'], row['diag_version'], row['fixture_no'],
             row['data_source']
        ) for row in unique_rows]

        if values:
            print(f"First record values tuple: {values[0]}")
            print(f"Values tuple length: {len(values[0])}")
            for i, val in enumerate(values[0]):
                print(f"  [{i}] {val} (type: {type(val)})")
            print()

        execute_values(cursor, insert_query, values, page_size=1000)
        conn.commit()
        cursor.close()

        print(
            f"Import completed for {os.path.basename(file_path)}. "
            f"Processed {len(unique_rows):,} deduplicated rows with ON CONFLICT DO NOTHING."
        )

        try:
            os.remove(file_path)
            print(f"Deleted XLSX file: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Could not delete XLSX file: {e}")
            sys.exit(1)

        sys.exit(0)

    except Exception as e:
        conn.rollback()
        print(f"Error importing {os.path.basename(file_path)}: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
