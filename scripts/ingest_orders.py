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

# Load environment variables
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

PG_HOST = os.getenv("BUFFALO_DB_HOST")
PG_PORT = int(os.getenv("BUFFALO_DB_PORT"))
PG_DB = os.getenv("BUFFALO_DB_NAME")
PG_USER = os.getenv("BUFFALO_DB_USER")
PG_PASS = os.getenv("BUFFALO_DB_PASS")

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
    with get_pg_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_raw (
                id BIGINT PRIMARY KEY,
                data JSONB NOT NULL
            )
        """)
        conn.commit()

# Endpoints
GET_KEY_URL = "https://index.buffaloex.com/buffalo/getRsaPublicKey"
LOGIN_URL = "https://index.buffaloex.com/buffalo/login"

def create_session():
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
    })

    # Get RSA public key
    resp = session.get(GET_KEY_URL)
    resp.raise_for_status()
    rsa_key = RSA.import_key(base64.b64decode(resp.text.strip()))

    # Encrypt password
    cipher = PKCS1_v1_5.new(rsa_key)
    encrypted_bytes = cipher.encrypt(PASSWORD.encode("utf-8"))
    encrypted_password_encoded = urllib.parse.quote(
        base64.b64encode(encrypted_bytes).decode("utf-8"), safe=''
    )

    # Login
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

def get_all_order_ids(session):
    page_size = 15
    url = f"https://index.buffaloex.com/mobileapi/myorder/orderList?condition=&status=0&pageNum=1&language=en&tableIndex=0"
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
        all_order_ids.extend([int(item["id"]) for item in order_list if "id" in item])
    return all_order_ids

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

def ingest_orders():
    init_db()
    session = create_session()
    all_order_ids = get_all_order_ids(session)
    print(f"Total order IDs fetched from API: {len(all_order_ids)}")

    new_orders_count = 0
    with get_pg_connection() as conn, conn.cursor() as cur:
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

    print(f"New orders inserted into Postgres: {new_orders_count}")
    print("Ingestion complete.")

if __name__ == "__main__":
    ingest_orders()
