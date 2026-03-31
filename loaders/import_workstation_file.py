import sys
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Add the parent directory to the path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE

UNIQUE_KEY_FIELDS = (
    'sn',
    'pn',
    'customer_pn',
    'workstation_name',
    'history_station_start_time',
    'history_station_end_time',
    'hours',
    'service_flow',
    'model',
    'history_station_passing_status',
    'passing_station_method',
    'operator',
    'first_station_start_time',
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

def build_unique_key(row):
    return tuple(row[field] for field in UNIQUE_KEY_FIELDS)

def main():
    if len(sys.argv) != 2:
        print("Usage: python import_workstation_file.py /path/to/file.xlsx", file=sys.stderr)
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Importing {file_path} into workstation_master_log...")
    conn = connect_to_db()

    try:
        df = pd.read_excel(file_path)
        df.columns = [clean_column_name(col) for col in df.columns]
        df['data_source'] = 'workstation'

        # Initial spreadsheet-level cleanup.
        dedup_cols = [c for c in df.columns if c not in ['day', 'tat', 'outbound_version']]
        original_count = len(df)
        df = df.drop_duplicates(subset=dedup_cols)
        cleaned_count = len(df)

        if original_count != cleaned_count:
            print(
                f"Cleaned {original_count - cleaned_count:,} duplicate rows "
                f"(ignoring 'day', 'tat', and 'outbound_version' columns)"
            )
            print(f"Original rows: {original_count:,}, Cleaned rows: {cleaned_count:,}")

        mapped_data = []
        for _, row in df.iterrows():
            history_station_end_time = normalize_datetime(row.get('history_station_end_time'))
            history_station_start_time = normalize_datetime(row.get('history_station_start_time'))
            if history_station_start_time is None:
                history_station_start_time = history_station_end_time

            mapped_row = {
                'sn': normalize_string(row.get('sn')),
                'pn': normalize_string(row.get('pn')),
                'customer_pn': normalize_string(row.get('customer_pn')),
                'workstation_name': normalize_string(row.get('workstation_name')),
                'history_station_start_time': history_station_start_time,
                'history_station_end_time': history_station_end_time,
                'hours': normalize_string(row.get('hours')),
                'service_flow': normalize_string(row.get('service_flow')),
                'model': normalize_string(row.get('model')),
                'history_station_passing_status': normalize_string(row.get('history_station_passing_status')),
                'passing_station_method': normalize_string(row.get('passing_station_method')),
                'operator': normalize_string(row.get('operator')),
                'first_station_start_time': normalize_datetime(row.get('first_station_start_time')),
                'data_source': 'workstation',
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
            sys.exit(0)

        cursor = conn.cursor()

        print(f"About to upsert {len(unique_rows):,} deduplicated records...")
        for i, row in enumerate(unique_rows[:3]):
            print(f"Record {i + 1}:")
            for key, value in row.items():
                print(f"  {key}: {value} (type: {type(value)})")
            print()

        insert_query = """
        INSERT INTO workstation_master_log (
            sn, pn, model, workstation_name,
            history_station_start_time, history_station_end_time, history_station_passing_status, operator, customer_pn,
            hours, service_flow, passing_station_method, first_station_start_time, data_source
        ) VALUES %s
        ON CONFLICT ON CONSTRAINT workstation_unique_constraint DO NOTHING
        """

        values = [(
            row['sn'], row['pn'], row['model'], row['workstation_name'],
            row['history_station_start_time'], row['history_station_end_time'], row['history_station_passing_status'], row['operator'], row['customer_pn'],
            row['hours'], row['service_flow'], row['passing_station_method'], row['first_station_start_time'], row['data_source']
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
