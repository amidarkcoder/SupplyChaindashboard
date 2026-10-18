from dataclasses import dataclass,astuple
import random
from datetime import date,datetime,timedelta
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

@dataclass
class Purchase_order:
    po_number: int
    suppiler_id : int
    product_id: int
    order_date: date
    promised_date: date
    delivery_date: date
    ordered_qty: int
    reveived_qty: int
    po_unit_price: int
    invoice_unit_price: int

load_dotenv()
from dim_suppiler import gen_suppiler
from dim_product import gen_product
from dim_warehouse import gen_warehouse

supp_list = gen_suppiler(int(os.getenv("suppiler_num")))
prod_list = gen_product(int(os.getenv("product_num")))
ware_list = gen_warehouse(int(os.getenv("warehouse_num")))

def fact_po(count = None) -> list[Purchase_order]:
    if count is None:
        count  = int(os.getenv("po_num", "2000000"))
    else:
        count = int(count)
    
    st_dt = datetime(2024,1,1)
    fn_dt =datetime(2026,12,31)
    st_ti = int(st_dt.timestamp())
    fn_ti = int(fn_dt.timestamp())
    s_value = random.choice(supp_list)
    p_list= random.choice(prod_list)
    w_list= random.choice(ware_list)
    po_list = []

    for _ in range(count):
        i = random.random()
        po_number = _
        order_date = date.fromtimestamp(random.randrange(st_ti,fn_ti))
        promised_date = order_date + timedelta(days=30)
        delivery_date = (promised_date - timedelta(days = 5)
                        if i < 0.5
                        else promised_date + timedelta(days= 5) )
        ordered_qty = random.randint(200, 5000)
        reveived_qty = ordered_qty - random.randint(0,10)
        po_unit_price = round(random.uniform(20.00,75.00),2)
        invoice_unit_price = po_unit_price

        po_list.append(Purchase_order(
            po_number=po_number,
            suppiler_id=s_value.suppiler_id,
            product_id=p_list.product_id,
            order_date=order_date,
            promised_date=promised_date,
            delivery_date=delivery_date,
            ordered_qty=ordered_qty,
            reveived_qty=reveived_qty,
            po_unit_price=po_unit_price,
            invoice_unit_price=invoice_unit_price
        ))
    return po_list

def save_po(purchase_data: list[Purchase_order]):
    connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )

    cursor = connection.cursor()

    tuples = [
        (p.po_number,p.suppiler_id,p.product_id,p.order_date,p.promised_date,
         p.delivery_date,p.ordered_qty,p.reveived_qty,p.po_unit_price,p.invoice_unit_price)
        for p in purchase_data
    ]
    query = """
            insert into fact.fact_purchase_order(po_number,suppiler_id, product_id, 
            order_date, promised_date, delivery_date, ordered_qty, reveived_qty,po_unit_price,invoice_unit_price)
            values %s
            """

    try:
        execute_values(cursor,query,tuples)
        connection.commit()
    except:
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    count = os.getenv("po_num","2000")
    data = fact_po(count)
    save_po(data)



