from pyspark.sql import SparkSession

# def get_company_crate_distribution(spark: SparkSession):
#     """
#     Calculates the distribution of crate types per company (number of orders per type) and registers it as a temporary view.
#     In case of duplicate companies stored under multiple IDs, one name of the company is used - 
#     (for eg: Healthy Snacks v/s Healthy Snacks Co, selected one name for the report to eliminate duplicate. 

#     :param spark: SparkSession object
#     :return: Transformed DataFrame with crate type distribution per company.
#     """
#     sql_query = f"""
#         WITH get_unique_company_name as 
#         (
#             select Company_id, company_name 
#             from (
#                 select company_id, company_name, ROW_NUMBER() OVER (PARTITION BY Company_id ORDER BY company_name asc) AS row_num 
#                 from orders_vw
#                 )
#             where row_num=1
#         )
#         SELECT 
#             ord.company_id,
#             gc.company_name, 
#             ord.crate_type, 
#             count(ord.order_id) as crate_type_distribution 
#         FROM orders_vw ord inner join get_unique_company_name gc on ord.company_id = gc.company_id
#         GROUP BY 1, 2, 3
#         ORDER BY 1, 2
#     """

#     df_company_crate_distribution = spark.sql(sql_query)
    
pass