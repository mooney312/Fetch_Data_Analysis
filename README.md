# Fetch Data Analysis
## Problem Statement


  What are the requirements?

    Review unstructured JSON data and diagram a new structured relational data model
        1. brands.json.gz
        2. users.json.gz
        3. [receipts.json.gz].https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/receipts.json.gz 
    Generate a query that answers a predetermined business question
    Generate a query to capture data quality issues against the new structured relational data model
    Write a short email or Slack message to the business stakeholder



## Highlevel Solution & Approach

  1. **Data Infrastructure**
        - Setup databricks runtime as the base image to process data using PySpark, Python and SQL.
        - Set up python and java environment variables just in case. 
        - Use Jupyter Notebook and data profiling and business queries.
          
  2. **Data Model**.
        - Analyze and create a data model
          
  3. **Data Ingestion**.
        - Ingest users JSON into a dataframe and create a "users" view. 
        - Ingest brands JSON into a dataframe and create a "brands" view. 
        - Ingest receipts JSON into a dataframe & create "receipts" and "receipts_item" views.
          
  4. **Data Profiling**
        -  Evaluate for data quality issues such as null values, duplicates and other inconsistencies and missing values.
    
  5. **Query to answer business questions**


[Data Model] ([https://app.diagrams.net/?mode=github](https://github.com/mooney312/Fetch_Data_Analysis/blob/main/Fetch_Data_Model-Page-2.drawio.svg)


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
     
