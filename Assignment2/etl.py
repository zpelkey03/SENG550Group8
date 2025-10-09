import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PGConnection
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

AS_OF_QUERY = """
SELECT *
FROM as_of_orders
ORDER BY order_id;
"""


def fetch_rows(conn: PGConnection):
    with conn, conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(AS_OF_QUERY)
        return c.fetchall()


def upsert_rows(collection: Collection, rows: list[dict[str, any]]):
    upserts = 0
    for r in rows:
        doc = {
            "order_id": r["order_id"],
            "order_date": r["order_date"],
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "customer_city": r["customer_city"],
            "product_id": r["product_id"],
            "product_name": r["product_name"],
            "product_price": float(r["product_price"]),
            "amount": float(r["amount"]),
        }
        collection.update_one({"order_id": doc["order_id"]}, {"$set": doc}, upsert=True)
        upserts += 1

    return upserts


def main():

    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGO_DB")
    mongo_collection = os.getenv("MONGODB_COLLECTION")

    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_collection]

    try:
        rows = fetch_rows(conn)
        print(f"{len(rows)} rows fetched from Postgres")

        collection.create_index(
            [("order_id", ASCENDING)], unique=True, name="unique_order_id"
        )

        upserted = upsert_rows(collection, rows)

        print(f"Upserted {str(upserted)} rows")

    finally:
        conn.close()
        client.close()


main()
