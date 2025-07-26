import psycopg2
import re
import csv
import pandas as pd
from datetime import datetime, timedelta

DB_PARAMS = {
    'dbname': 'backup',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}
# Pattern: one letter, followed by 7 digits, followed by one letter
pattern = re.compile(r'^[A-Za-z]\d{7}[A-Za-z]$')
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
# File paths (update as needed)
input_file_path = 'TelenumberJuly21.txt'
input_csv_path = 'Telenumber_migration.csv'
valid_file_path = 'Telenumber_migration_valid.csv'
invalid_file_path = 'Telenumber_migration_invalid.csv'
prod_existing_data = 'prod_existing_data.csv'
records_to_be_inserted_intermediate = 'records_to_be_inserted_intermediate.csv'
legacy_file = 'Telenumber_migration_Legacy.csv'
avl_file = 'Telenumber_migration_Available.csv'
quarantine_file = 'Telenumber_migration_Quarantine.csv'
invalid_status_file = 'Telenumber_migration_invalid_status.csv'

def convertToCsv():
    with open(input_file_path, 'r') as txt_file, \
            open(input_csv_path, 'w', newline='') as csv_file:

        writer = csv.writer(csv_file)

        for line_num, line in enumerate(txt_file, start=1):
            # Split on any whitespace (spaces, tabs, etc.)
            columns = line.strip().split()
            if len(columns) == 2:  # Skip empty lines
                writer.writerow(columns)
            else:
                print(f"Line {line_num}: Incorrect number of columns - {line.strip()}\n")
    print(f"Conversion complete. Output saved to - {input_csv_path}\n")

def create_valid_InvalidFiles():
    with open(input_csv_path, 'r') as infile, \
            open(valid_file_path, 'w') as valid_file, \
            open(invalid_file_path, 'w') as invalid_file:

        for line in infile:
            columns = line.split(',')
            if columns and pattern.match(columns[0]):
                valid_file.write(line)
            else:
                invalid_file.write(line)
    print(f"Processing complete for create_valid_InvalidFiles method. Output saved to - {valid_file_path} and {invalid_file_path}\n")

def fetch_existing_service_ids():
    print("Attempting to fetch existing service_ids from prod...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT service_id FROM service_fnn.service_fnn")
    rows = cur.fetchall()
    with open(prod_existing_data, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['service_id'])
        for row in rows:
            writer.writerow([row[0]])
    cur.close()
    conn.close()
    print("fetch_existing_service_ids completed...")

def compare_and_filter_serviceIds():
    print("compare_and_filter_serviceIds started...")
    try:
        df_prod = pd.read_csv(prod_existing_data)
        df_valid = pd.read_csv(valid_file_path)

        if 'service_id' not in df_prod.columns:
            raise ValueError(f"'{prod_existing_data}' must contain a 'service_id' column.")

        # Get the name of the first column in valid_file_path for comparison
        valid_df_first_col = df_valid.columns[0]

        print(f"Successfully loaded '{prod_existing_data}' with {len(df_prod)} rows.")
        print(f"Successfully loaded '{valid_file_path}' with {len(df_valid)} rows.")

        # 2. Sort both DataFrames
        df_prod_sorted = df_prod.sort_values(by='service_id').copy()
        print(f"'{prod_existing_data}' sorted by 'service_id'.")
        df_prod_sorted.to_csv("prod_existing_data_sorted.csv", index=False)
        df_valid_sorted = df_valid.sort_values(by=valid_df_first_col).copy()
        print(f"'{valid_file_path}' sorted by '{valid_df_first_col}'.")
        df_valid_sorted.to_csv("valid_file_sorted.csv", index=False)

        # For efficient lookup, convert 'service_id' from prod_existing_data to a set
        existing_service_ids = set(df_prod_sorted['service_id'].unique())
        print(f"Created a set of {len(existing_service_ids)} unique service IDs from existing data.")

        # 3. Compare and filter
        # Identify rows in df_valid_sorted whose first column value is NOT in existing_service_ids
        non_matching_records = df_valid_sorted[~df_valid_sorted[valid_df_first_col].isin(existing_service_ids)]

        print(f"Found {len(non_matching_records)} records in '{valid_file_path}' not present in '{prod_existing_data}'.")

        # 4. Write the entire non-matching rows to file3.csv
        if not non_matching_records.empty:
            non_matching_records.to_csv(records_to_be_inserted_intermediate, index=False)
            print(f"Non-matching records successfully written to '{records_to_be_inserted_intermediate}'.")
        else:
            print("No non-matching records found. No output file was created.")

    except FileNotFoundError as e:
        print(f"Error: One of the input files was not found. Please check the paths. {e}")
    except ValueError as e:
        print(f"Data error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def create_load_files():
    print("create_load_files started...")
    ageout_format = '%Y-%m-%d %H:%M:%S.%f'

    with open(records_to_be_inserted_intermediate, 'r') as infile, \
            open(legacy_file, 'w', newline='') as legacy_out, \
            open(avl_file, 'w', newline='') as avl_out, \
            open(quarantine_file, 'w', newline='') as quarantine_out, \
            open(invalid_status_file, 'w', newline='') as invalid_status_out:

        legacy_writer = csv.writer(legacy_out)
        avl_writer = csv.writer(avl_out)
        quarantine_writer = csv.writer(quarantine_out)
        invalid_status_writer = csv.writer(invalid_status_out)

        # Write headers
        legacy_writer.writerow(['service_id', 'status', 'ageout_ts', 'created_by', 'created_ts', 'updated_by', 'updated_ts', 'version'])
        avl_writer.writerow(['service_id', 'status', 'ageout_ts', 'created_by', 'created_ts', 'updated_by', 'updated_ts', 'version'])
        quarantine_writer.writerow(['service_id', 'status', 'ageout_ts', 'created_by', 'created_ts', 'updated_by', 'updated_ts', 'version'])
        invalid_status_writer.writerow(['service_id', 'status'])
        created_ts = datetime.now().strftime(ageout_format)[:-3]
        for line in infile:
            columns = line.strip().split(',')
            if len(columns) != 2:
                continue  # skip malformed lines

            service_id, status = columns[0], columns[1]
            if status in ('R', 'A', 'C'):
                legacy_writer.writerow([service_id, 'LEGACY', '', 'admin', created_ts, '', '', '0'])
            elif status == 'V':
                avl_writer.writerow([service_id, 'AVAILABLE', '', 'admin', created_ts, '', '', '0'])
            elif status == 'D':
                ageout_date = (datetime.now() + timedelta(days=90)).strftime(ageout_format)[:-3]
                quarantine_writer.writerow([service_id, 'QUARANTINE', ageout_date, 'admin', created_ts, '', '', '0'])
            else:
                invalid_status_writer.writerow([service_id, status])
    print(f"Legacy file: {legacy_file}")
    print(f"Available file: {avl_file}")
    print(f"Quarantine file: {quarantine_file}")

def insert_records_to_service_fnn_table():
    copy_sql = f"""
        COPY {"service_fnn.service_fnn"} (service_id, status, ageout_ts, created_by, created_ts, updated_by, updated_ts, version)
        FROM STDIN WITH CSV HEADER
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cur, open(legacy_file, 'r', encoding='utf-8') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        print(f"✅ Successfully imported LEGACY data ")
        with conn.cursor() as cur, open(avl_file, 'r', encoding='utf-8') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        print(f"✅ Successfully imported AVAILABLE data ")
        with conn.cursor() as cur, open(quarantine_file, 'r', encoding='utf-8') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        print(f"✅ Successfully imported QUARANTINE data ")
    except Exception as e:
        print(f" Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


def insert_all_records_to_telenumber_migration():
    # Adjust columns to match your CSV and table definition
    copy_sql = f"""
        COPY {"service_fnn.telenumber_migration"} (service_id, status)
        FROM STDIN WITH CSV HEADER
    """

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cur, open("mydata.csv", 'r', encoding='utf-8') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        print(f"✅ Successfully imported data ")
    except Exception as e:
        print(f" Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print(f"Started at {datetime.now().strftime('%Y%m%d_%H%M%S')}")
    #convertToCsv()
    #create_valid_InvalidFiles()
    #fetch_existing_service_ids()
    #compare_and_filter_serviceIds()
    create_load_files()
    #insert_records_to_service_fnn_table()
    #insert_all_records_to_telenumber_migration()
    print(f"Finished at {datetime.now().strftime('%Y%m%d_%H%M%S')}")
