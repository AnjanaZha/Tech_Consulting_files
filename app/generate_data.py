import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import pandas as pd
import os


# --- Database Connection Details ---
DB_NAME = "fraud_detect"
DB_USER = "root"
DB_PASS = "root"
DB_HOST = "postgres"  # Service name from docker-compose
DB_PORT = "5432"

# --- Local Output Directory ---
OUTPUT_DIR = "/data/output"  # Mounted volume path in Docker (ensure it's mapped in docker-compose)
LOCAL_FILE = "transactions_output.csv"

def setup_database():
    """Drops old tables and creates the new normalized table structure."""
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    # Drop tables in reverse order of dependency
    cur.execute("DROP TABLE IF EXISTS transactions;")
    cur.execute("DROP TABLE IF EXISTS users;")
    cur.execute("DROP TABLE IF EXISTS payment_methods;")
    
    # Create Users table
    cur.execute("""
        CREATE TABLE users (
            user_id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            join_date TIMESTAMP NOT NULL
        );
    """)
    
    # Create Payment Methods table
    cur.execute("""
        CREATE TABLE payment_methods (
            method_id SERIAL PRIMARY KEY,
            method_type TEXT NOT NULL,
            card_number TEXT
        );
    """)

    # Create Transactions table



    cur.execute("""
        CREATE TABLE transactions (
            transaction_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(user_id),
            method_id INTEGER REFERENCES payment_methods(method_id),
            transaction_amount FLOAT NOT NULL,
            transaction_timestamp TIMESTAMP NOT NULL,
            is_fraud BOOLEAN NOT NULL
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database tables created successfully.")

def generate_and_populate_data():
    """Generates synthetic data and inserts it into the database and saves locally."""
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    fake = Faker()
    
    # --- Populate Users and Payment Methods ---
    user_ids = []
    for _ in range(100):  # Generate 100 fake users
        cur.execute("INSERT INTO users (username, join_date) VALUES (%s, %s) RETURNING user_id;", 
                    (fake.user_name(), fake.date_time_this_year()))
        user_ids.append(cur.fetchone()[0])
        
    method_ids = []
    for _ in range(50):  # Generate 50 fake payment methods
        cur.execute("INSERT INTO payment_methods (method_type, card_number) VALUES (%s, %s) RETURNING method_id;",
                    (random.choice(['Credit Card', 'Debit Card', 'PayPal']), fake.credit_card_number()))
        method_ids.append(cur.fetchone()[0])

    conn.commit()
    print("Users and payment methods populated.")

    # --- Populate Transactions ---
    transactions_data = []
    for _ in range(5000):  # Generate 5,000 transactions
        is_fraud = random.random() < 0.05  # 5% fraudulent
        transaction_amount = round(random.uniform(1.0, 500.0), 2)
        transaction_timestamp = datetime.now() - timedelta(minutes=random.randint(1, 100000))
        
        transaction_row = (
            random.choice(user_ids),
            random.choice(method_ids),
            transaction_amount,
            transaction_timestamp,
            is_fraud
        )
        transactions_data.append(transaction_row)
        


    insert_query = """
        INSERT INTO transactions (user_id, method_id, transaction_amount, transaction_timestamp, is_fraud)
        VALUES (%s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, transactions_data)
    conn.commit()
    print(f"{len(transactions_data)} transaction records were inserted successfully.")
    
    # --- Save data to local CSV ---
    df = pd.DataFrame(transactions_data, columns=['user_id', 'method_id', 'transaction_amount', 'transaction_timestamp', 'is_fraud'])
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, LOCAL_FILE)
    df.to_csv(file_path, index=False)
    print(f"Transactions also saved locally at: {file_path}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    setup_database()
    generate_and_populate_data()
