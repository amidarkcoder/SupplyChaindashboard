from dataclasses import dataclass
import random
from faker import Faker
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

@dataclass
class Product:
    product_id : int
    product_name: str
    category: str
    unit_cost: float

def gen_product(count: int) -> list[Product]:
    count = int(count)
    fake = Faker()
    categorys = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Sports", "Beauty"]
    product_list = []
    for _ in range(count):
        product_id = _
        product_name = fake.catch_phrase().title()
        category = random.choice(categorys)
        unit_cost = round(random.uniform(10.00, 500.00),2)

        product_list.append(Product(
            product_id=product_id,
            product_name=product_name,
            category=category,
            unit_cost=unit_cost
        ))
        
    return product_list

def save_product(product_data):
    connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )
    
    tuples = [
        (p.product_id,p.product_name,p.category,p.unit_cost)
        for p in product_data
        ]

    query = """
        insert into dim.dim_product(product_id,product_name,category,unit_cost)
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
    count = int(os.getenv("product_num","200"))
    data = gen_product(count)
    save_product(data)
    
    
