import os
import requests
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
from dotenv import load_dotenv
import json
import urllib.parse
import math
import psycopg2
from psycopg2.extras import Json

# =========================
# Load environment variables
# =========================
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

PG_HOST = os.getenv("BUFFALO_DB_HOST", "localhost")
PG_PORT = int(os.getenv("BUFFALO_DB_PORT", "5432"))
PG_DB = os.getenv("BUFFALO_DB_NAME", "buffalo_orders_db")
PG_USER = os.getenv("BUFFALO_DB_USER", "airflow")
PG_PASS = os.getenv("BUFFALO_DB_PASS", "airflow_password")

if not USERNAME or not PASSWORD:
    raise ValueError("USERNAME or PASSWORD not found in .env file")

# Postgres database setup

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )

def init_db():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders_raw (
            id TEXT PRIMARY KEY,
            data JSONB NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# Endpoints

GET_KEY_URL = "https://index.buffaloex.com/buffalo/getRsaPublicKey"
LOGIN_URL = "https://index.buffaloex.com/buffalo/login"

# Create session & login

def create_session():
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
    })

    # Step 1: Get RSA key
    resp = session.get(GET_KEY_URL)
    resp.raise_for_status()
    public_key_b64 = resp.text.strip()
    rsa_key = RSA.import_key(base64.b64decode(public_key_b64))

    # Step 2: Encrypt password
    cipher = PKCS1_v1_5.new(rsa_key)
    encrypted_bytes = cipher.encrypt(PASSWORD.encode("utf-8"))
    encrypted_password_encoded = urllib.parse.quote(
        base64.b64encode(encrypted_bytes).decode("utf-8"), safe=''
    )

    # Step 3: Login
    login_payload = {"username": USERNAME, "password": encrypted_password_encoded}
    login_resp = session.post(
        LOGIN_URL,
        headers={"Content-Type": "application/json;charset=UTF-8"},
        data=json.dumps(login_payload)
    )
    login_resp.raise_for_status()
    ticket = login_resp.json().get("data", {}).get("ticket")
    if not ticket:
        raise ValueError("No ticket returned from login response")
    session.headers.update({"Authorization": ticket, "Buffalo-Ticket": ticket})
    return session


# Fetch all order IDs from Buffalo api

def get_all_order_ids(session):
    page_size = 15
    url = "https://index.buffaloex.com/mobileapi/myorder/orderList?condition=&status=0&pageNum=1&language=en&tableIndex=0"
    resp_json = session.get(url).json()
    result_map = resp_json.get("data", {}).get("resultMap", {})
    record_total = result_map.get("recordTotal", 0)
    if record_total == 0:
        return []

    total_pages = math.ceil(record_total / page_size)
    all_order_ids = []

    for page in range(1, total_pages + 1):
        page_url = f"https://index.buffaloex.com/mobileapi/myorder/orderList?condition=&status=0&pageNum={page}&language=en&tableIndex=0"
        resp = session.get(page_url).json()
        order_list = resp.get("data", {}).get("resultMap", {}).get("list", [])
        all_order_ids.extend([item["id"] for item in order_list if "id" in item])
    return all_order_ids


# Fetch order details

def fetch_order_details(order_id, session):
    url = f"https://index.buffaloex.com/mobileapi/myorder/detail/{order_id}?language=en"
    headers = {"Referer": f"https://index.buffaloex.com/client/orders/order-detail?id={order_id}"}
    resp = session.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        data["_id"] = order_id
        return data
    print(f"Failed to fetch details for ID {order_id}: {resp.status_code}")
    return None


# Main ingestion function

def ingest_orders():
    init_db()
    session = create_session()
    all_order_ids = get_all_order_ids(session)
    print(f"Total order IDs fetched from API: {len(all_order_ids)}")

    conn = get_pg_connection()
    cur = conn.cursor()
    new_orders_count = 0

    for order_id in all_order_ids:
        cur.execute("SELECT 1 FROM orders_raw WHERE id = %s", (order_id,))
        if not cur.fetchone():
            raw_order = fetch_order_details(order_id, session)
            if raw_order:
                cur.execute(
                    "INSERT INTO orders_raw (id, data) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                    (order_id, Json(raw_order))
                )
                new_orders_count += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"New orders inserted into Postgres: {new_orders_count}")
    print("Ingestion complete.")


if __name__ == "__main__":
    ingest_orders()
