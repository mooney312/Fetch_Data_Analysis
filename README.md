# Fetch Data Analysis

## What are the requirements?

   - Review the unstructured JSON data and diagram a new structured relational data model
        1. brands.json.gz
        2. users.json.gz
        3. [receipts.json.gz].https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/receipts.json.gz 
  - Generate a query that answers a predetermined business questions:
        1. What are the top 5 brands by receipts scanned for most recent month?
        2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
        3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
        4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
        5. Which brand has the most spend among users who were created within the past 6 months?
        6. Which brand has the most transactions among users who were created within the past 6 months?

  - Generate a query to capture data quality issues against the new structured relational data model
  - Write a short email or Slack message to the business stakeholder



## Highlevel Solution & Approach

  1. **Data Infrastructure**
        - Setup the data infrastrure for processing the data and executing queries for the excercide:
          -  Use databricks runtime as the base image to process data using PySpark, Python and SQL.
          -  Set up python and java environment variables just in case. 
          -  Use Jupyter Notebook and data profiling and business queries.
          
  2. **Data Model**.
        - Analyze the data and create a data model
        - Data is read as pyspark dataframe, and create raw views/table for the source raw data (Optional)
            1. users_raw
            2. brands_raw
            3. receipts_raw
               
        ![Data Model](https://github.com/mooney312/Fetch_Data_Analysis/blob/main/Fetch_Data_Model-Page-2.drawio.svg)

        - Data is read as pyspark dataframe, and create flattened views/table for the data 
            1. users
            2. brands
            3. receipts
            4. receipt_item

        ![Data Model](https://github.com/mooney312/Fetch_Data_Analysis/blob/main/Fetch_Data_Model-Page-2.drawio.svg)
               
          
  2. **Data Ingestion**.
         Ingesting the raw data: 
            - Ingest users JSON into a dataframe and create a raw view "users_raw". 
            - Ingest brands JSON into a dataframe and create a raw view "brands_raw". 
            - Ingest receipts JSON into a dataframe and create a raw view "receipts_raw".
         Create flattened and de-normalized views for queries: 
            - Ingest users JSON into a dataframe and create a flattened "users" view. 
            - Ingest brands JSON into a dataframe and create a flattened "brands" view. 
            - Ingest receipts JSON into a dataframe and create a flattened "receipts" and "receipts_item" views.
          
  5. **Data Profiling**
        -  Evaluate for data quality issues such as null values, duplicates and other inconsistencies and missing values.
    
  6. **Query to answer business questions**



## Assumptions
  - For the current application requirement, the source data both orders and invoices are static files.
  - No file format changes expected for the source filesv- the `orders` data is CSV format, `invoices` data is in JSON format.
  - No schema changes expected for both source data files.
  - No requirement to perisist the data for future use. 
  - Using Windows and enabling WSL 2 for running docker.

## Improvements & Next Steps
  
  -  Ingesting the source data from realtime API or from a database or an automated process for production use.
  -  Persiting the data in database for reuability and making data available across teams and geographies.
  -  Use `Docker-Compose.yml` to deal with persisting data, multiple services, shared volumes, environment variables, or networking.
  -  Use data visulization tool such as Tableau for more dashboard capabilities as data is persisted in database.
  -  Change the base image to optimized Databricks image for better performance.
  -  Create purpose built dimension data models such as facts and dimensions or denormalised tables for better query and report performance.

## Reference Links

  1. [Dockerfile](https://github.com/Lenin-Subramonian/data-engineering-ifco_test/blob/main/Docker/Dockerfile)
  2. [Jupyter Notebook Git Link].(https://github.com/Lenin-Subramonian/data-engineering-ifco_test/blob/main/notebooks/IFCO_Data_Analysis.ipynb)
     
