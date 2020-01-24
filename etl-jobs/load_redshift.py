import sys
import pg
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['db_name', 'host', 'port', 'user', 'password', 'glue_db', 'iam_role'])
glue_db = args['glue_db']
iam_role = args['iam_role']

create_external_schema = f"""
    create external schema if not exists external_{glue_db}
    from data catalog 
    database '{glue_db}'
    iam_role '{iam_role}'
    create external database if not exists;
"""

create_sales_schema = 'create schema if not exists sales'

create_time_dim_table = '''
    create table if not exists sales.time_dim(
        time_sk timestamp not null,
        primary key(time_sk)
    )
'''

create_date_dim_table = '''
    create table if not exists sales.date_dim(
        date_sk date not null,
        primary key(date_sk)
    )
'''

create_customer_dim_table = '''
    create table if not exists sales.customer_dim(
        customer_sk varchar not null,
        unique_id varchar,
        primary key(customer_sk)
    )
'''

create_location_dim_table = '''
    create table if not exists sales.location_dim(
        zip_code_prefix_sk varchar not null,
        city varchar,
        state varchar,
        primary key(zip_code_prefix_sk)
    )
'''

create_product_dim_table = '''
    create table if not exists sales.product_dim(
        product_sk varchar not null,
        category_name varchar,
        name_length integer,
        description_length integer,
        photos_qty integer,
        weight_g integer,
        length_cm integer,
        height_cm integer,
        width_cm integer,
        primary key(product_sk)
    )
'''

create_seller_dim_table = '''
    create table if not exists sales.seller_dim(
        seller_sk varchar not null,
        primary key(seller_sk)
    )
'''

create_sales_fact_table = '''
    create table if not exists sales.sales_fact(
        customer_sk varchar not null,
        seller_sk varchar not null,
        product_sk varchar not null,
        seller_zip_code_prefix_sk varchar not null,
        customer_zip_code_prefix_sk varchar not null,
        purchase_date_sk varchar not null,
        purchase_time_sk varchar not null,
        approved_at_date_sk varchar not null,
        approved_at_time_sk varchar not null,
        delivered_carrier_date_sk varchar not null,
        delivered_carrier_time_sk varchar not null,
        delivered_customer_date_sk varchar not null,
        delivered_customer_time_sk varchar not null,
        order_estimated_delivery_date_sk varchar not null,
        order_status varchar,
        total_cost decimal,
        amount decimal,
        freight decimal,
        primary key(
            customer_sk,
            seller_sk,
            product_sk,
            seller_zip_code_prefix_sk,
            customer_zip_code_prefix_sk,
            purchase_date_sk,
            purchase_time_sk,
            approved_at_date_sk,
            approved_at_time_sk,
            delivered_carrier_date_sk,
            delivered_carrier_time_sk,
            delivered_customer_date_sk,
            delivered_customer_time_sk,
            order_estimated_delivery_date_sk
        ),
        foreign key(customer_sk) references sales.customer_dim(customer_sk),
        foreign key(seller_sk) references sales.seller_dim(seller_sk),
        foreign key(product_sk) references sales.product_dim(product_sk),
        foreign key(seller_zip_code_prefix_sk) references sales.location_dim(zip_code_prefix_sk),
        foreign key(customer_zip_code_prefix_sk) references sales.location_dim(zip_code_prefix_sk),
        foreign key(purchase_date_sk) references sales.date_dim(date_sk),
        foreign key(purchase_time_sk) references sales.time_dim(time_sk),
        foreign key(approved_at_date_sk) references sales.date_dim(date_sk),
        foreign key(approved_at_time_sk) references sales.time_dim(time_sk),
        foreign key(delivered_carrier_date_sk) references sales.date_dim(date_sk),
        foreign key(delivered_carrier_time_sk) references sales.time_dim(time_sk),
        foreign key(delivered_customer_date_sk) references sales.date_dim(date_sk),
        foreign key(delivered_customer_time_sk) references sales.time_dim(time_sk),
        foreign key(order_estimated_delivery_date_sk) references sales.date_dim(date_sk)
    )
'''

insert_customer_dim = f'''
    insert into sales.customer_dim (
        select c.customer_id, c.customer_unique_id 
        from external_{glue_db}.olist_customers c
    )
'''

insert_seller_dim = f'''
    insert into sales.seller_dim (
        select s.seller_id
        from external_{glue_db}.olist_sellers s
    )
'''

insert_product_dim = f'''
    insert into sales.product_dim (
        select p.product_id, p.product_category_name, p.product_name_length, p.product_description_length,
               p.product_photos_qty, p.product_weight_g, p.product_length_cm, p.product_height_cm, 
               p.product_width_cm
        from external_{glue_db}.olist_products p
    )
'''

insert_location_dim = f'''
    insert into sales.location_dim (
        select c.customer_zip_code_prefix, c.customer_city, c.customer_state
        from external_{glue_db}.olist_customers c
        union
        select s.seller_zip_code_prefix, s.seller_city, s.seller_state
        from external_{glue_db}.olist_sellers s
    )
'''

insert_time_dim = f'''
    insert into sales.time_dim (
        select cast('1900-01-01 ' || to_char(cast(nvl(o1.order_purchase_timestamp,'1900-01-01 00:00:00') as timestamp),
          'HH24:MI:SS') as timestamp) 
        from external_{glue_db}.olist_orders o1
        union
        select cast('1900-01-01 ' || to_char(cast(nvl(o2.order_approved_at,'1900-01-01 00:00:00') as timestamp), 
          'HH24:MI:SS') as timestamp)
        from external_{glue_db}.olist_orders o2
        union
        select cast('1900-01-01 ' || to_char(cast(nvl(o3.order_delivered_carrier_date,'1900-01-01 00:00:00') as timestamp), 
          'HH24:MI:SS') as timestamp)
        from external_{glue_db}.olist_orders o3
        union
        select cast('1900-01-01 ' || to_char(cast(nvl(o4.order_delivered_customer_date,'1900-01-01 00:00:00') as timestamp), 
          'HH24:MI:SS') as timestamp)
        from external_{glue_db}.olist_orders o4
    )
'''

insert_date_dim = f'''
    insert into sales.date_dim (
        select trunc(cast(nvl(o1.order_purchase_timestamp,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_orders o1
        union
        select trunc(cast(nvl(o2.order_approved_at,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_orders o2
        union
        select trunc(cast(nvl(o3.order_delivered_carrier_date,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_orders o3
        union
        select trunc(cast(nvl(o4.order_delivered_customer_date,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_orders o4
        union
        select trunc(cast(nvl(o5.order_estimated_delivery_date,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_orders o5
    )
'''

insert_sales_fact = f'''
    insert into sales.sales_fact (
        select 
            o.customer_id,
            i.seller_id,
            i.product_id,
            s.seller_zip_code_prefix,
            c.customer_zip_code_prefix,
            trunc(cast(nvl(o.order_purchase_timestamp,'1900-01-01 00:00:00') as timestamp)),
            cast('1900-01-01 ' || to_char(cast(nvl(o.order_purchase_timestamp,'1900-01-01 00:00:00') as timestamp), 'HH24:MI:SS') as timestamp),
            trunc(cast(nvl(o.order_approved_at,'1900-01-01 00:00:00') as timestamp)),
            cast('1900-01-01 ' || to_char(cast(nvl(o.order_approved_at,'1900-01-01 00:00:00') as timestamp), 'HH24:MI:SS') as timestamp),
            trunc(cast(nvl(o.order_delivered_carrier_date,'1900-01-01 00:00:00') as timestamp)),
            cast('1900-01-01 ' || to_char(cast(nvl(o.order_delivered_carrier_date,'1900-01-01 00:00:00') as timestamp), 'HH24:MI:SS') as timestamp),
            trunc(cast(nvl(o.order_delivered_customer_date,'1900-01-01 00:00:00') as timestamp)),
            cast('1900-01-01 ' || to_char(cast(nvl(o.order_delivered_customer_date,'1900-01-01 00:00:00') as timestamp), 'HH24:MI:SS') as timestamp),
            trunc(cast(nvl(o.order_estimated_delivery_date,'1900-01-01 00:00:00') as timestamp))
        from external_{glue_db}.olist_order_items i
        join external_{glue_db}.olist_orders o on i.order_id = o.order_id
        join external_{glue_db}.olist_sellers s on i.seller_id = s.seller_id
        join external_{glue_db}.olist_customers c on o.customer_id = c.customer_id
    )
'''

conn_string = f'host={args["host"]} port={args["port"]} dbname={args["db_name"]} user={args["user"]} password={args["password"]}'
conn = pg.connect(dbname=conn_string)

print('Creating schemas')
conn.query(create_external_schema)
conn.query(create_sales_schema)
conn.query(create_time_dim_table)
conn.query(create_date_dim_table)
conn.query(create_customer_dim_table)
conn.query(create_location_dim_table)
conn.query(create_product_dim_table)
conn.query(create_seller_dim_table)
conn.query(create_sales_fact_table)


db = pg.DB(conn)
db.start()

print('Processing customers')
db.query(insert_customer_dim)
print('Processing sellers')
db.query(insert_seller_dim)
print('Processing products')
db.query(insert_product_dim)
print('Processing locations')
db.query(insert_location_dim)
print('Processing times')
db.query(insert_time_dim)
print('Processing dates')
db.query(insert_date_dim)
print('Processing sales')
db.query(insert_sales_fact)

db.commit()
conn.close()