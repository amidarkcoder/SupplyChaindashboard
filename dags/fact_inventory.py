from dataclasses import dataclass,astuple
import random
from datetime import date,datetime,timedelta
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

@dataclass
class Inventory:
    snapshot_date: date
    warehouse_id: int
    product_id: int
    stock_qty: int

from dim_warehouse import gen_warehouse
from dim_product import gen_product
ware_id = gen_warehouse(os.getenv("warehouse_num"))
prod = gen_product(os.getenv("product_num"))

def ft_inven(count: int) -> list[Inventory]:
    count = int(count)
    st_dt = datetime(2024,1,1)
    fn_dt =datetime(2026,12,31)
    st_ti = int(st_dt.timestamp())
    fn_ti = int(fn_dt.timestamp())
    w_list = ware_id
    p_list = prod
    number_of_days = (fn_dt - st_dt).days + 1
    max_unique_row = number_of_days * len(w_list) * len(p_list)
    if count > max_unique_row:
        raise ValueError("number of row is high")
    
    inventory_list = []
    seen_keys = set()
    while len(inventory_list) < count:
        snapshot_date = st_dt + timedelta(
            days=random.randrange(number_of_days)
        )
        warehouse = random.choice(w_list)
        product = random.choice(p_list)
        warehouse_id = warehouse.warehouse_id
        product_id = product.product_id
        key = (snapshot_date,warehouse_id,product_id)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        stock_qty =random.randint(-20,140)

        inventory_list.append(Inventory(
            snapshot_date=snapshot_date,
            warehouse_id=warehouse_id,
            product_id=product_id,
            stock_qty=stock_qty
        ))
    return inventory_list

def save_inventory(inventory_data: list[Inventory]):
    connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )


    tuples = [
        (i.snapshot_date,i.warehouse_id,i.product_id,i.stock_qty)
        for i in inventory_data
    ]

    query = """
            insert into fact.fact_inventory(snapshot_date, warehouse_id, product_id, stock_qty)
            values %s
            ON CONFLICT (snapshot_date, warehouse_id, product_id)
            DO UPDATE SET stock_qty = EXCLUDED.stock_qty
        """

    try:
        with connection.cursor()as cursor:
            execute_values(cursor, query, tuples)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"error:{e}")
        raise
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    count = os.getenv("inventory_num","40000")
    data = ft_inven(count)
    save_inventory(data)