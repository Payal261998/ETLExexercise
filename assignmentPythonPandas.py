import pandas as pd
import sqlite3

conn = sqlite3.connect('C:\Users\User\Downloads\S30 ETL Assignment v22-01 (1) (1)\S30 ETL Assignment.db')


cust_df = pd.read_sql_query("SELECT * from customers", conn)
orders_df = pd.read_sql_query("SELECT * from orders", conn)
sales_df = pd.read_sql_query("SELECT * from sales", conn)
items_df = pd.read_sql_query("SELECT * from items", conn)

cust_sales_df = cust_df.merge(sales_df,on="customer_id",how="inner")
sales_orders_df = orders_df.merge(cust_sales_df,on="sales_id",how="inner")
orders_items_df = items_df.merge(sales_orders_df,on="item_id",how="inner")

orders_items_df_not_null = orders_items_df[orders_items_df.quantity.notnull()]

final_df = orders_items_df_not_null.groupby('customer_id','age','item_name')['quantity'].sum().reset_index()
final_result = final_df[(final_df.age >18) & (final_df.age < 35)]


final_result.to_csv('AssignmentSolution.csv', sep=';', index = False)
