from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

@dataclass
class Warehouse:
    warehouse_id: int
    warehouse_name: str
    city: str

def gen_warehouse(count: int) -> list[Warehouse]:
    count = int(count)
    cities = [
        "Tokyo", "New York", "London", "Paris", "Sydney",
        "Cairo", "Rio de Janeiro", "Mumbai", "Toronto", "Berlin",
        "Beijing", "Mexico City", "Buenos Aires", "Rome", "Bangkok",
        "Istanbul", "Seoul", "Lagos", "Singapore", "Dubai",
        "Los Angeles", "Madrid", "Moscow", "Nairobi", "Jakarta",
        "Amsterdam", "Vienna", "San Francisco", "Barcelona", "Stockholm"
        ]
    count = int(count)
    warehouse_list = []

    if not cities:
        raise ValueError("cities must contain at least one city")

    for idx in range(count):
        warehouse_id = idx + 1
        city = cities[idx % len(cities)]
        warehouse_name = f"{warehouse_id}-{city}"

        warehouse_list.append(
            Warehouse(
                warehouse_id=warehouse_id,
                warehouse_name=warehouse_name,
                city=city,
            )
        )

    return warehouse_list

def save_warehouse(warehouse_data: list[Warehouse]):
    connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )
    tuples = [
        (w.warehouse_id, w.warehouse_name, w.city)
        for w in warehouse_data
    ]

    query = """
            insert into dim.dim_warehouse(warehouse_id, warehouse_name, city)
            values %s
        """

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, query, tuples)
        connection.commit() 
    except Exception as e:
        connection.rollback()
        raise print(f"error:{e}")

    finally:
        connection.close()

if __name__ == "__main__":
    count = os.getenv("warehouse_num","60")
    data = gen_warehouse(count)
    save_warehouse(data)
