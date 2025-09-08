import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

POSTGRES_CONFIG = {
    "host": os.getenv("BUFFALO_DB_HOST", "localhost"),
    "port": int(os.getenv("BUFFALO_DB_PORT", 5432)),
    "dbname": os.getenv("BUFFALO_DB_NAME", "buffalo_orders_db"),
    "user": os.getenv("BUFFALO_DB_USER", "airflow"),
    "password": os.getenv("BUFFALO_DB_PASS", "airflow_password"),
}

def fetch_raw_orders():
    query = "SELECT id, data FROM orders_raw"
    with psycopg2.connect(**POSTGRES_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()  # [(id, json), ...]

def flatten(order_id, data):
    # Extract top-level order_id if nested
    if isinstance(order_id, dict) and "$numberLong" in order_id:
        order_id = str(order_id["$numberLong"])

    # Extract boxList -> detaillist info
    boxlist = data.get("boxList") or []
    declared_number, declared_value = None, None
    if boxlist and "detaillist" in boxlist[0]:
        details = boxlist[0]["detaillist"]
        if details:
            declared_number = details[0].get("number")
            # fallback to actualdeclaredvalue if declaredvalue is missing
            declared_value = details[0].get("declaredvalue") or details[0].get("actualdeclaredvalue")

    # Convert create time string to datetime
    create_time = None
    if data.get("createtimeStr"):
        try:
            create_time = datetime.strptime(data["createtimeStr"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    return {
        "order_id": int(order_id),
        "express_number": data.get("expressnumber"),
        "third_number": data.get("thirdnumber"),
        "pay_status_name": data.get("paystatusname"),
        "tax_pay_status_name": data.get("taxpaystatusname"),
        "status_name": data.get("statusname"),
        "receive_address": data.get("receiveaddress"),
        "ascertained_weight": data.get("ascertainedweight"),
        "ascertained_volum_weight": data.get("ascertainedvolumweight"),
        "ascertained_cost": data.get("ascertainedcost"),
        "final_weight": data.get("finalweight"),
        "create_time": create_time,
        "declared_number": declared_number,
        "declared_value": declared_value,
    }

def init_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS orders_transformed (
        order_id BIGINT PRIMARY KEY,
        express_number TEXT,
        third_number TEXT,
        pay_status_name TEXT,
        tax_pay_status_name TEXT,
        status_name TEXT,
        receive_address TEXT,
        ascertained_weight NUMERIC,
        ascertained_volum_weight NUMERIC,
        ascertained_cost NUMERIC,
        final_weight NUMERIC,
        create_time TIMESTAMP,
        declared_number NUMERIC,
        declared_value NUMERIC
    );
    """
    with psycopg2.connect(**POSTGRES_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def load(flat_orders):
    insert = """
    INSERT INTO orders_transformed (
        order_id, express_number, third_number, pay_status_name,
        tax_pay_status_name, status_name, receive_address,
        ascertained_weight, ascertained_volum_weight, ascertained_cost,
        final_weight, create_time, declared_number, declared_value
    ) VALUES %s
    ON CONFLICT (order_id) DO NOTHING;
    """
    rows = [
        (
            o["order_id"],
            o["express_number"],
            o["third_number"],
            o["pay_status_name"],
            o["tax_pay_status_name"],
            o["status_name"],
            o["receive_address"],
            o["ascertained_weight"],
            o["ascertained_volum_weight"],
            o["ascertained_cost"],
            o["final_weight"],
            o["create_time"],
            o["declared_number"],
            o["declared_value"],
        )
        for o in flat_orders
    ]
    with psycopg2.connect(**POSTGRES_CONFIG) as conn, conn.cursor() as cur:
        execute_values(cur, insert, rows)
        conn.commit()

def main():
    init_table()
    raw_orders = fetch_raw_orders()
    
    # Parse JSON if it's stored as text
    parsed_orders = []
    for oid, data in raw_orders:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except:
                continue
        parsed_orders.append((oid, data))

    flat_orders = [flatten(order_id, data) for order_id, data in parsed_orders]
    load(flat_orders)
    print(f"Loaded {len(flat_orders)} orders into orders_transformed")

if __name__ == "__main__":
    main()
