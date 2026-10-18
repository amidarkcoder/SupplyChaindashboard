from dataclasses import dataclass
import random
from datetime import date,datetime
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

@dataclass
class Sales:
    sales_date: date
    warehouse_id: int
    product_id: int
    qty_sold: int
    sales_value: float

from dim_warehouse import gen_warehouse
from dim_product import gen_product

ware_id = gen_warehouse(os.getenv("warehouse_num"))
prod = gen_product(os.getenv("product_num"))

def ft_sales(count: int) -> list[Sales]:
    count = int(count)

    st_dt = datetime(2024,1,1)
    fn_dt =datetime(2026,12,31)
    st_ti = int(st_dt.timestamp())
    fn_ti = int(fn_dt.timestamp())

    sales_list = []

    for _ in range(count):
        sales_date = date.fromtimestamp(random.randrange(st_ti,fn_ti))
        w_item = random.choice(ware_id)
        p_item = random.choice(prod)
        qty_sold = random.randint(5,25)
        sales_value = round(p_item.unit_cost * qty_sold, 2)

        sales_list.append(Sales(
            sales_date=sales_date,
            warehouse_id=w_item.warehouse_id,
            product_id=p_item.product_id,
            qty_sold= qty_sold,
            sales_value=sales_value
        ))
    return sales_list

def save_sales(sales_data: list[Sales]):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432")
    )

    cursor = conn.cursor()

    query = """
        insert into fact.fact_sales(sale_date, warehouse_id, product_id, qty_sold, sales_value)
        values %s
    """
    tuples = [
        (s.sales_date,s.warehouse_id,s.product_id,s.qty_sold,s.sales_value)
        for s in sales_data
    ]
    try:
        execute_values(cursor, query, tuples)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"{e}")
        raise
    finally:
        cursor.close()
        conn.close()
if __name__ ==  "__main__":
    count = os.getenv("sales_num","2000")
    data = ft_sales(count)
    save_sales(data)    
    
