create table if not exists dim.dim_product(
    product_id int primary key,
    product_name varchar(150),
    category varchar(50),
    unit_cost NUMERIC(10,2)
);

create table if not exists dim.dim_suppiler(
    suppiler_id int primary key,
    suppiler_name varchar(50),
    city varchar(50),
    category varchar(50)
);

create table if not exists dim.dim_warehouse(
    warehouse_id int primary key,
    warehouse_name varchar(50),
    city varchar(50)
);

