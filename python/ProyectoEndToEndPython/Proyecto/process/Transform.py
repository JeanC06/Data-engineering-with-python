
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'


    def prod_total_sales(self, customers_df, orders_df, order_items_df, products_df, categories_df, departments_df):
        q = """
            SELECT
                d.department_name,
                c.category_name,
                p.product_name,
                SUM(oi.order_item_subtotal) AS total_sales
            FROM
                departments_df AS d
                INNER JOIN categories_df AS c ON d.department_id = c.category_department_id
                INNER JOIN products_df AS p ON c.category_id = p.product_category_id
                INNER JOIN order_items_df AS oi ON p.product_id = oi.order_item_product_id
                INNER JOIN orders_df AS o ON oi.order_item_order_id = o.order_id
                INNER JOIN customers_df AS cu ON o.order_customer_id = cu.customer_id
            WHERE
                o.order_status <> 'CANCELED'
            GROUP BY
                d.department_name,
                c.category_name,
                p.product_name
            ORDER BY
                total_sales DESC
            LIMIT 10
            """
        result = sqldf(q)

        return result
    
    def customer_total_s(self, customers_df, orders_df, order_items_df):
        q = """
            SELECT
                c.customer_id,
                c.customer_fname,
                c.customer_lname,
                COUNT(o.order_id) AS total_orders,
                SUM(oi.order_item_subtotal) AS total_sales
            FROM
                customers_df AS c
                INNER JOIN orders_df AS o ON c.customer_id = o.order_customer_id
                INNER JOIN order_items_df AS oi ON o.order_id = oi.order_item_order_id
            WHERE
                o.order_status = 'COMPLETE'
            GROUP BY
                c.customer_id,
                c.customer_fname,
                c.customer_lname
            ORDER BY
                total_sales DESC
            LIMIT 10
            """
        result = sqldf(q)

        return result