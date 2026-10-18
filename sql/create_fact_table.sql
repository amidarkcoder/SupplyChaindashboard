create table if not exists fact.fact_inventory(
    snapshot_date timestamp,
    warehouse_id INTEGER NOT NULL,FOREIGN KEY (warehouse_id) REFERENCES dim.dim_warehouse (warehouse_id),
    product_id int not null, foreign key (product_id) references dim.dim_product(product_id),
    stock_qty int
);

create table if not exists fact.fact_purchase_order(
    po_number int,
    suppiler_id int not null,foreign key(suppiler_id) references dim.dim_suppiler(suppiler_id),
    product_id int not null, foreign key(product_id) references dim.dim_product(product_id),
    order_date timestamp ,
    promised_date timestamp,
    delivery_date timestamp,
    ordered_qty int,
    reveived_qty int,
    po_unit_price int,
    invoice_unit_price int
);
create table if not exists fact.fact_sales(
    sale_date timestamp,
    warehouse_id int NOT NULL,FOREIGN KEY (warehouse_id) REFERENCES dim.dim_warehouse (warehouse_id),
    product_id int not null, foreign key(product_id) references dim.dim_product(product_id),
    qty_sold int,
    sales_value NUMERIC(10,2)
);

