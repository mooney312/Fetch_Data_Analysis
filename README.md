# Fetch Data Analysis

## What are the requirements?

   - Review the unstructured JSON data and diagram a new structured relational data model
        1. [brands.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/brands.json.gz)
        2. [users.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/users.json.gz)
        3. [receipts.json.gz](https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/receipts.json.gz) 
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
        Analyze the data and create a data model
        - Data is read as pyspark dataframe, and create raw views/table for the source raw data (Optional)
            1. users_raw
            2. brands_raw
            3. receipts_raw
               
        ![Data Model - Raw Data](https://github.com/mooney312/Fetch_Data_Analysis/blob/main/Fetch_Data_Model-Raw.drawio.svg)

        - Data is read as pyspark dataframe, and create flattened views/table for the data 
            1. users
            2. brands
            3. receipts
            4. receipt_item

        ![Data Model](https://github.com/mooney312/Fetch_Data_Analysis/blob/main/Fetch_Data_Model-Page-2.drawio.svg)
               
          
  2. **Data Ingestion**.
     
        - Ingesting the raw data:
           - Ingest users JSON into a dataframe and create a raw view "users_raw". 
           - Ingest brands JSON into a dataframe and create a raw view "brands_raw". 
           - Ingest receipts JSON into a dataframe and create a raw view "receipts_raw".
        - Create flattened and de-normalized views for queries: 
           - Ingest users JSON into a dataframe and create a flattened "users" view. 
           - Ingest brands JSON into a dataframe and create a flattened "brands" view. 
           - Ingest receipts JSON into a dataframe and create a flattened "receipts" and "receipts_item" views.
          
  5. **Data Profiling and Data Quality**
     
        - Evaluate for data quality issues such as null values, duplicates and other inconsistencies and missing values.
        - Refer [Jupyter Notebook for the queries]()
           1. Formatting Issue:
               - User data JSON does not conform to a valid JSON syntax - unable to read JSON lines with line-delimited mode. 
               - Requires additional data clean up and enforcing to process line by line. 
               - Not recommended for large datasets and impact scalability and to enable streaming.
               - user records not processed due to invalid JSON format (corrupt records)
           2. Evaluate NULL values.
           3. Evaluate duplicates values in "user" table - Duplicates in User table
              - Duplicates in Brands table
               ![image](https://github.com/user-attachments/assets/a7598427-375a-4898-be8a-b902bd8d6545)

           4. Evaluate data accuracy and integrity
              - Junk or invalid barcode are associated to receipts. The barcode is 12 digit in length, but receipts have invalid barcode values.
                 - Incorrect or invalid barcode values causing the `brands` table and `receipts_item` join to fail and returns no value.
                 - This data issue is impacting the query to generate <ins>_Top 5 brands by receipt scanned_</ins>
              - Brand code is null in many cases. Barcode and Brandcode identifies the unique in brands data set.  So, having NULL in brandcode will cause join issues and data quality issues in downstream process.
              - Missing data: The count of items `rewardsReceiptItemList` and `purchasedItemCount` is not matching
                
               ![image](https://github.com/user-attachments/assets/f95c6b90-f103-4c54-b897-7af5d7b328b7)

    
  7. **Business Questions**

      **Question**
   
      1. What are the top 5 brands by receipts scanned for most recent month?
              - Use receipt Item table & brand table - brand name, receipts id, scanned date
              - Need to check if barcode and brandCode combo is unique or just barcode is required lookuo brand name. 
      2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
              - Use receipt Item table & brand table - brand name, receipts id, scanned date
              - Need to check if barcode and brandCode combo is unique or just barcode is required lookuo brand name.              
   
      **Assessment**:

         For the  most recent month and prior month, in this case the recent month is "2021-03" and prior month is "2021-02", the query returms no value or null value due to data quality issues in "barcode" column in receipts. Incorrect or invalid barcode values causing the `brands` table and `receipts_item` join to fail and returns no value. 
         The below table shows the list of top brand barcodes based on receipts scanned. Please review the "barcodes" here - none of the bar codes are valid.
   
      ![image](https://github.com/user-attachments/assets/fcf6ccfc-bdeb-413a-9205-0fa1a2424180)

      **Question**

      3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
          - Use receipt table - rewardsReceiptStatus and totalspent columns. 
      4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
          - Use receipt table - rewardsReceiptStatus and purchasedItemCount columns    
      _Assmuption_: There is no status "Accepted", instead "FINISHED" statusis used as "accepted" status.

      **Assessment**:

      - Average spend for "FINISHED" status is greater than that of "REJECTED" status, 15.86046511627907 v/s 2.5441176470588234
      - Total number of items purchased from each receipts for "FINISHED" status is greater than that of "REJECTED" status, 8184 v/s 173

      ![image](https://github.com/user-attachments/assets/1751807c-19db-4ed7-8df0-47c6b165bd48)

      **Question**

         5. Which brand has the most spend among users who were created within the past 6 months?
             - Use receipt Item table, user table abd brand table - brand name, receipts id, user create date
         6. Which brand has the most transactions among users who were created within the past 6 months?
             - Use receipt Item table, user table abd brand table - brand name, receipts id, user create date     
   
      **Assessment**:
   
         - Tostitos and Swanson are the top brand based on users spend among users who were created within the past 6 months (7527.79 vs 7187.14)
         - Tostitos and Swanson are the tied at top based on most users transactions among users who were created within the past 6 months (11 each)
         
         Top 5 brand based on users spend and users transactions are given below table.
         ![image](https://github.com/user-attachments/assets/a9977e09-3528-421e-9e8c-1fafa6080a0b)



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
     
