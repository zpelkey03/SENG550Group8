import os
from dotenv import load_dotenv
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.extensions import connection as PGConnection

# Table creation queries for sql

# Fact table
create_fact_sales_sql = """
CREATE TABLE IF NOT EXISTS fact_sales (
    order_number INT,
    order_line_number INT,
    product_key INT REFERENCES dim_product(product_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    date_key INT REFERENCES dim_date(date_key),
    quantity_ordered INT,
    price_each NUMERIC,
    sales NUMERIC,
    deal_size VARCHAR
);
"""

#Dimension tables
create_dim_product_sql = """
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_code VARCHAR,
    product_line VARCHAR,
    msrp NUMERIC
);
"""

create_dim_customer_sql = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_name VARCHAR,
    contact_first_name VARCHAR,
    contact_last_name VARCHAR,
    phone VARCHAR,
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    territory VARCHAR
);
"""

create_dim_date_sql = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    order_date DATE,
    qtr_id INT,
    month_id INT,
    year_id INT,
    month_name VARCHAR,
    day INT,
    day_of_week VARCHAR
);
"""

#Get DB connection
def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    return conn

#Create the tables
def create_tables(cursor):
    # Create all tables
    for query in [create_dim_product_sql, create_dim_customer_sql, create_dim_date_sql, create_fact_sales_sql]:
        cursor.execute(query)

def load_csv(path):
    return pd.read_csv(path, encoding='latin1', parse_dates=['ORDERDATE'])

def build_dimensions(df):
    #Create the product dimension table
    dim_product = df[['PRODUCTCODE','PRODUCTLINE','MSRP']].drop_duplicates().reset_index(drop=True)
    dim_product['product_key'] = dim_product.index + 1

    #Create the customer dimension table
    dim_customer = df[['CUSTOMERNAME','CONTACTFIRSTNAME','CONTACTLASTNAME','PHONE',
                       'ADDRESSLINE1','ADDRESSLINE2','CITY','STATE','POSTALCODE','COUNTRY','TERRITORY']].drop_duplicates().reset_index(drop=True)
    dim_customer['customer_key'] = dim_customer.index + 1

    #Create the date dimension table
    dim_date = df[['ORDERDATE','QTR_ID','MONTH_ID','YEAR_ID']].drop_duplicates().reset_index(drop=True)
    dim_date['date_key'] = dim_date.index + 1
    dim_date['month_name'] = dim_date['ORDERDATE'].dt.month_name()
    dim_date['day'] = dim_date['ORDERDATE'].dt.day
    dim_date['day_of_week'] = dim_date['ORDERDATE'].dt.day_name()

    return dim_product, dim_customer, dim_date

#Builds the fact table based on the dimension tables
def build_fact(df, dim_product, dim_customer, dim_date):
    fact_sales = df.merge(dim_product, on=['PRODUCTCODE','PRODUCTLINE','MSRP']) \
                   .merge(dim_customer, on=['CUSTOMERNAME','CONTACTFIRSTNAME','CONTACTLASTNAME','PHONE',
                                            'ADDRESSLINE1','ADDRESSLINE2','CITY','STATE','POSTALCODE','COUNTRY','TERRITORY']) \
                   .merge(dim_date, on=['ORDERDATE','QTR_ID','MONTH_ID','YEAR_ID'])
    fact_sales = fact_sales[['ORDERNUMBER','ORDERLINENUMBER','product_key','customer_key','date_key',
                             'QUANTITYORDERED','PRICEEACH','SALES','DEALSIZE']]
    fact_sales.columns = ['order_number','order_line_number','product_key','customer_key','date_key',
                          'quantity_ordered','price_each','sales','deal_size']
    return fact_sales

#Runs the insert queries into the SQL tables
def insert_dataframe(cursor, table_name, df, columns):
    print(df.columns)
    values = [tuple(x) for x in df[columns].to_numpy()]
    cols_str = ','.join(columns)
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s"
    execute_values(cursor, query, values)

#Runs the ETL pipeline
def etl_pipeline(csv_path):
    conn = get_connection()
    cursor = conn.cursor()
    
    create_tables(cursor)
    conn.commit()
    
    df = load_csv(csv_path)
    #Create the dimension tables and fact table
    dim_product, dim_customer, dim_date = build_dimensions(df)
    fact_sales = build_fact(df, dim_product, dim_customer, dim_date)

    #Product column renaming to match table schema
    dim_product.rename(columns={
    'PRODUCTCODE': 'product_code',
    'PRODUCTLINE': 'product_line'
    }, inplace=True)

    #customer column renaming to match table schema
    dim_customer.rename(columns={
    'CUSTOMERNAME': 'customer_name',
    'CONTACTFIRSTNAME': 'contact_first_name',
    'CONTACTLASTNAME': 'contact_last_name',
    'PHONE': 'phone',
    'ADDRESSLINE1': 'address_line1',
    'ADDRESSLINE2': 'address_line2',
    'POSTALCODE': 'postal_code',
    'CITY': 'city',
    'STATE': 'state',
    'COUNTRY': 'country',
    'TERRITORY': 'territory'
    }, inplace=True)

    #date column renaming to match table schema
    dim_date.rename(columns={
    'ORDERDATE': 'order_date',
    'QTR_ID': 'qtr_id',
    'MONTH_ID': 'month_id',
    'YEAR_ID': 'year_id',
    }, inplace=True)


    insert_dataframe(cursor, 'dim_product', dim_product, ['product_code','product_line','MSRP'])
    insert_dataframe(cursor, 'dim_customer', dim_customer, ['customer_name','contact_first_name','contact_last_name',
                                                            'phone','address_line1','address_line2','city','state',
                                                            'postal_code','country','territory'])
    insert_dataframe(cursor, 'dim_date', dim_date, ['order_date','qtr_id','month_id','year_id','month_name','day','day_of_week'])
    print(dim_product.head())
    insert_dataframe(cursor, 'fact_sales', fact_sales, ['order_number','order_line_number','product_key','customer_key',
                                                        'date_key','quantity_ordered','price_each','sales','deal_size'])
    
    conn.commit()
    cursor.close()
    conn.close()
    print("ETL complete!")

if __name__ == "__main__":
 load_dotenv()
 etl_pipeline(os.getenv("CSV_PATH"))