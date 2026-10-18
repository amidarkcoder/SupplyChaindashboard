from dataclasses import dataclass
from typing import Optional
import random
import os
from faker import Faker
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

@dataclass
class Suppiler:
    suppiler_id : int
    suppiler_name: str
    city: str
    category: str

def gen_suppiler(count: int) -> list[Suppiler]:
    count = int(count)
    fake = Faker()
    cities = [
    "Tokyo", "New York", "London", "Paris", "Sydney",
    "Cairo", "Rio de Janeiro", "Mumbai", "Toronto", "Berlin",
    "Beijing", "Mexico City", "Buenos Aires", "Rome", "Bangkok",
    "Istanbul", "Seoul", "Lagos", "Singapore", "Dubai",
    "Los Angeles", "Madrid", "Moscow", "Nairobi", "Jakarta",
    "Amsterdam", "Vienna", "San Francisco", "Barcelona", "Stockholm"
    ]
    categorys = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Sports", "Beauty"]
    supplier_list = []
    for _ in range(count):
        suppiler_id = _
        suppiler_name = fake.name()
        city = random.choice(cities)
        category = random.choice(categorys)

        supplier_list.append(Suppiler(
            suppiler_id=suppiler_id,
            suppiler_name=suppiler_name,
            city=city,
            category=category
        ))
    return supplier_list

def save_suppiler(suppiler_data):
    connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )

    tuples = [
        (s.suppiler_id,s.suppiler_name,s.city,s.category)
        for s in suppiler_data
    ]

    query = """
            insert into dim.dim_suppiler(suppiler_id, suppiler_name, city, category)
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
    count = os.getenv("suppiler_num","20")
    data = gen_suppiler(count)
    save_suppiler(data)
    

    
    
