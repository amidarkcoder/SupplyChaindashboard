from datetime import date,datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
 

load_dotenv()

from fact_inventory import ft_inven,save_inventory
from fact_purchase_orders import fact_po,save_po
from fact_sales import ft_sales, save_sales
from silver import to_silver
def task_inventory():
    data = ft_inven(os.getenv("inventory_num"))
    save_inventory(data)
def task_po():
    data = fact_po(os.getenv("po_num"))
    save_po(data)
def task_sale():
    data = ft_sales(os.getenv("sale_num"))
    save_sales(data)
def task_silver():
    to_silver()
with DAG(
    dag_id = "SCM",
    schedule="*/3 * * * *",
    start_date= datetime(2026,10,16)
) as dag:


    t_inventory = PythonOperator(
            task_id = "run_inventory",
            python_callable = task_inventory
        )

    t_po = PythonOperator(
        task_id = "run_po",
        python_callable = task_po
    )

    t_sale = PythonOperator(
            task_id = "run_sale",
            python_callable = task_sale
        )

    t_silver = PythonOperator(
        task_id = "run_silver",
        python_callable = task_silver
    )

    t_inventory >> t_po >> t_sale >> t_silver






    