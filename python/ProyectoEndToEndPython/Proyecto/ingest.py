from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
#import pandas as pd


extract = Extract()
transform = Transform()
load = Load()

categories_df_db = extract.read_mysql("categories", 'retail_db')
customers_df_db = extract.read_mysql("customers", 'retail_db')
departments_df_db = extract.read_mysql("departments", 'retail_db')
order_items_df_db = extract.read_mysql("order_items", 'retail_db')
orders_df_db = extract.read_mysql("orders", 'retail_db')
products_df_db = extract.read_mysql("products", 'retail_db')

print("Extración base de datos")

load.load_to_cloud_storage(categories_df_db,"jeanproyecto","landing/categories")
load.load_to_cloud_storage(customers_df_db,"jeanproyecto","landing/customers")
load.load_to_cloud_storage(departments_df_db,"jeanproyecto","landing/departments")
load.load_to_cloud_storage(order_items_df_db,"jeanproyecto","landing/order_items")
load.load_to_cloud_storage(orders_df_db,"jeanproyecto","landing/orders")
load.load_to_cloud_storage(products_df_db,"jeanproyecto","landing/products")

print("Carga capa landing")

customers_df = extract.read_cloud_storage("jeanproyecto","landing/customers")
categories_df = extract.read_cloud_storage("jeanproyecto","landing/categories")
departments_df = extract.read_cloud_storage("jeanproyecto","landing/departments")
order_items_df = extract.read_cloud_storage("jeanproyecto","landing/order_items")
orders_df = extract.read_cloud_storage("jeanproyecto","landing/orders")
products_df = extract.read_cloud_storage("jeanproyecto","landing/products")

print("extracción de la capa landing")

a=transform.prod_total_sales(customers_df, orders_df, order_items_df, products_df, categories_df, departments_df)
b=transform.customer_total_s(customers_df, orders_df, order_items_df)

print("Transformación realizada")

load.load_to_cloud_storage(a,"jeanproyecto","gold/product_tot_sales")
load.load_to_cloud_storage(b,"jeanproyecto","gold/customer_total_s")

print("Carga de datos a capa gold")

product_tot = extract.read_cloud_storage("jeanproyecto","gold/product_tot_sales")
customer_tot = extract.read_cloud_storage("jeanproyecto","gold/customer_total_s")

print("extracción de la capa gold")

load.load_to_mysql(product_tot, "product_tot" , "dbprocesada")
load.load_to_mysql(customer_tot, "customer_tot" , "dbprocesada")

print("carga a base de datos Mysql")