**Fourth: Communicate with Stakeholders**

Construct an email or slack message that is understandable to a product or business leader who isn’t familiar with your day to day work. 
This part of the exercise should show off how you communicate and reason about data with others. Commit your answers to the git repository along with the rest of your exercise.

    What questions do you have about the data?
    How did you discover the data quality issues?
    What do you need to know to resolve the data quality issues?
    What other information would you need to help you optimize the data assets you're trying to create?
    What performance and scaling concerns do you anticipate in production and how do you plan to address them?

-----------------------------------------------------------------------------------------------------------------
Email
-----------------------------------------------------------------------------------------------------------------

Hi < Customer Success Lead>,

How are you? We are data services team and very excited to collaborate with the customer success team. 

Last week, we got a request from your team to ingest and analyze the "users", "brands" and "receipts" data.  The initial POC to ingest the data was successfull, and we were able analyze the data to answer the questions raised by your team. 

However, we observed that there are many data quality issues that might impact the outcome of this effort and our ability to accurately answers the questions. Here are some of the major data quality issues. The **critical** and **high** are one that needs immediate attention as that directly impact the project delivery, the solution and most importantly the accuracy of data. 

![image](https://github.com/user-attachments/assets/6f57ea2a-d748-48f9-a625-1d8839da33b3)

Additionally, to make this a scalable production ready product we also need to answer the below questions:

    1. Instead of ingesting from a static file, how to ingest directly from the source using realtime API or from a database or any other automated process? 
    2. Size of the data and number of expected transactions in a day etc.  
    3. Persiting the data and views in database for reuability and making data available across teams and geographies.
    4. Use data visulization tool such as Tableau for more dashboard capabilities as data is persisted in database.
    5. Create purpose built facts and dimensions or denormalised tables for better query and report performance.
    
Here are the answers to the questions your team have raised based on current data set. Please also note that the first 2 questions have a direct impact due to one of the data issues listed above. 
   
   **Questions**  
   
          1. What are the top 5 brands by receipts scanned for most recent month?
          2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?              
   **Assessment**:
   
           - For the  most recent month and prior month (recent month is "2021-03" and prior month is "2021-02"), the query returns no records or null value due to data quality issues in "barcode" column in receipts.
           - Incorrect or invalid barcode values causing the `brands` table and `receipts_item` join to fail and returns no value. 

The below table shows the list of top brand barcodes based on receipts scanned. Please review the "barcodes" here - none of the bar codes are valid.  
![image](https://github.com/user-attachments/assets/fcf6ccfc-bdeb-413a-9205-0fa1a2424180)

   **Questions**  
   
         3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
         4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
    
   **Assessment**:
   
        - **Assmuption**: There is no status "Accepted", instead "FINISHED" statusis used as "accepted" status.
        - Average spend for "FINISHED" status is greater than that of "REJECTED" status, 15.86046511627907 v/s 2.5441176470588234
        - Total number of items purchased from each receipts for "FINISHED" status is greater than that of "REJECTED" status, 8184 v/s 173

   ![image](https://github.com/user-attachments/assets/1751807c-19db-4ed7-8df0-47c6b165bd48)

   **Questions**

      5. Which brand has the most spend among users who were created within the past 6 months?
             - Use receipt Item table, user table abd brand table - brand name, receipts id, user create date
      6. Which brand has the most transactions among users who were created within the past 6 months?
             - Use receipt Item table, user table abd brand table - brand name, receipts id, user create date     
   
   **Assessment**:
   
       - Tostitos and Swanson are the top brand based on users spend among users who were created within the past 6 months (7527.79 vs 7187.14)
       - Tostitos and Swanson are the tied at top based on most users transactions among users who were created within the past 6 months (11 each)
         
   Top 5 brand based on users spend and users transactions are given below table.
   ![image](https://github.com/user-attachments/assets/a9977e09-3528-421e-9e8c-1fafa6080a0b)

   Let's connect and discuss these issues in detail. Let us know if you have any questions. 

   Thank you. 

   Data Services Team



