import pandas as pd
import sqlite3

conn = sqlite3.connect('C:\Users\User\Downloads\S30 ETL Assignment v22-01 (1) (1)\S30 ETL Assignment.db')

df = pd.read_sql('select c.customer_id,c.age,i.item_name as item,sum(o.quantity) as quantity from customers c join sales s on 
c.customer_id = s.customer_id join orders o on s.sales_id = o.sales_id join items i 
on o.item_id = i.item_id where o.quantity is not null and c.age > 18 and c.age < 35 
group by c.customer_id,c.age,i.item_name', conn)

df.to_csv('AssignmentSolution.csv', sep=';', index = False)