import psycopg2
import csv
import re
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'unms',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
FILE1_NAME = f"{"file1"}_{timestamp}.csv"
FILE2_NAME = f"{"file2"}_{timestamp}.csv"
FILE3_NAME = f"{"file3"}_{timestamp}.csv"


def fetch_service_ids():
    print("Attempting to connect to the PostgreSQL database...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT service_id FROM service_fnn.service_fnn")
    rows = cur.fetchall()
    with open(FILE1_NAME, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow([row[0]])
    cur.close()
    conn.close()
    print("fetch_service_ids completed...")

def extract_and_sort_digits():
    print("extract_and_sort_digits started...")
    digit_list = []
    with open(FILE1_NAME, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            match = re.search(r'(\d+)', row[0])
            if match:
                digits = match.group(1).zfill(7)
                digit_list.append(digits)
    digit_list = sorted(set(digit_list))
    with open(FILE2_NAME, 'w', newline='') as f:
        writer = csv.writer(f)
        for digits in digit_list:
            writer.writerow([digits])
    print("extract_and_sort_digits completed...")

def find_missing_numbers():
    print("find_missing_numbers started...")
    with open(FILE2_NAME, newline='') as f:
        existing = set(row[0] for row in csv.reader(f))
    all_numbers = {str(i).zfill(7) for i in range(0, 1000000)}
    missing = sorted(all_numbers - existing)
    with open(FILE3_NAME, 'w', newline='') as f:
        writer = csv.writer(f)
        for num in missing:
            writer.writerow([num])
    print("find_missing_numbers completed...")

def create_and_insert_pool():
    print("create_and_insert_pool started...")
    batch_size = 50000
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS service_fnn.service_fnn_available_pool (
                                                                                      service_id varchar NOT NULL,
                                                                                      state_code int4 NOT NULL,
                                                                                      created_ts timestamp NULL
                )
                """)
    now = datetime.now()
    with open(FILE3_NAME, newline='') as f:
        reader = csv.reader(f)
        batch = []
        for i, row in enumerate(reader, start=1):
            batch.append((row[0], int(row[0][0]), now))
            if i % batch_size == 0:
                args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode() for x in batch)
                cur.execute("INSERT INTO service_fnn.service_fnn_available_pool (service_id, state_code, created_ts) VALUES " + args_str)
                conn.commit()
                print(f"Inserted and committed batch ending at record {i}")
                batch = []

    # Insert any remaining records
        if batch:
            args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode() for x in batch)
            cur.execute("INSERT INTO service_fnn.service_fnn_available_pool (service_id, state_code, created_ts) VALUES " + args_str)
            conn.commit()
        print(f"Inserted and committed final batch with {len(batch)} records")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_service_ids()
    extract_and_sort_digits()
    find_missing_numbers()
    create_and_insert_pool()
