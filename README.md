# Spar-Nord-Bank-ETL-and-BI-Project
<img src="https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/Spar%20Node%20Bank%20Photo.jpg" alt="Spar Nord Bank Office photo" width="100%">

## Introduction

In this project, I've developed a batch ETL pipeline that extracts transactional data from Amazon RDS, transforms it into a usable format, and loads it into an Amazon S3 bucket.The data is then loaded into Redshift Tables. This allows us to run analytical queries on the data and gain valuable insights. I leverage a set of industry-standard tools and services, including:

**AWS EMR:** Harnesses the scalability of Amazon Elastic MapReduce (EMR) for efficient data processing and analysis.

**AWS RDS (MySQL):** Seamlessly stores and manages the structured data using Amazon RDS, a reliable and high-performance database service.

**Hadoop:** I utilize the robust Hadoop ecosystem for distributed data storage and processing.

**Apache Scoop:** Used to extract the transactional data from a MySQL RDS server to HDFS(EC2) instance .

**Apache PySpark:** PySpark is used for to transform the transactional data according to the target schema.

**AWS RedShift:** Amazon Redshift is employed for creating tables as per target schema and performing analysis queries on the data to gain insights .

**AWS S3:** Amazon S3 serves as a repository for holding the transformed data with in their respective tables.

## Problem Statement

* In this project, Spar Nord Bank aims to enhance ATM cash management by closely monitoring withdrawal patterns and the associated influencing factors. 
* The objective is to optimize the frequency of cash refills, taking into account variables such as ATM activity, geographical location, weather conditions, and the day of the week.
* Additionally, the project seeks to extract valuable insights from the data beyond just refill frequency optimization.

## Data
* The [dataset](https://www.kaggle.com/sparnord/datasets) is a ATM Transactions dataset from [Spar Nord Bank](https://www.sparnord.dk/om-spar-nord). The bank has roots as far back as 1824 and is today Denmark's 5th largest bank.
* This dataset comprises around **2.5 million records** of withdrawal data along with weather information at the time of the transactions from over **100 ATMs across Denmark from the year 2017**
* The [data dictionary](https://www.kaggle.com/datasets/sparnord/danish-atm-transactions) can be found [here](https://www.kaggle.com/datasets/sparnord/danish-atm-transactions)
* Spar Nord Bank has built a Dimensional Model datastore (ATM Data Mart) on this ATM transaction data to undustand usage pattern

## Approach

The project unfolds through a structured series of steps:
* **1. Data Extraction:** I initiate the process by extracting **transactional data** from a designated **MySQL RDS server**, seamlessly transferring it to an **HDFS instance** running on **EC2** using **Sqoop**.   For detailed information, check out the  [extraction process.](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/SqoopDataIngestion.pdf)
* **2. Data Transformation:** Next, I employ **PySpark** to transform the raw transactional data into a format that aligns with our target **schema**. More insights on this transformation are available [here.](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/SparkETLCode.ipynb)
* **3. Data Loading:** The transformed data finds its new home in an **Amazon S3 bucket**, where it's stored securely. Dive deeper into this step by exploring the loading process [here.](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/SparkETLCode.ipynb)
* **4. Redshift Table Creation:** To prepare for analysis, I create Redshift tables based on our target schema. This involves tasks like setting up a **Redshift Cluster**, establishing a **database** within the cluster, and executing queries to define **dimension and fact tables**. Detailed instructions can be found [here.](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/RedshiftSetup.pdf) 
* **5. Data Loading into Redshift:** I load the data stored in Amazon S3 into the corresponding Redshift tables within our cluster. Specifics on this procedure are outlined [here.](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/RedshiftSetup.pdf).
* **6. Analysis Queries:** Finally I perform analysis queries using **SQL** on the Redshift cluster. Find details [here](https://github.com/EddieAmaitum/Spar-Nord-Bank-ETL-and-BI-Project/blob/main/RedshiftQueries.pdf). I answer key **business questions** such as:
  *   Identifying the top 10 ATMs with the highest transaction volume in the 'inactive' state.
  *   Correlating ATM failures with recorded weather conditions during transactions.
  *   Ranking the top 10 ATMs by the total number of transactions throughout the year.
  *   Analyzing the monthly count of ATM transactions going inactive.
  *   Discovering the top 10 ATMs with the highest total amount withdrawn during the year.
  *   Examining failed ATM transactions across various card types.
  *   Presenting the top 10 records with transaction counts, sorted by ATM_number, ATM_manufacturer, location, weekend_flag, and total_transaction_count, both on weekdays and weekends throughout the year.
  *   Identifying the most active day for each ATM located in "Vejgaard".

For detailed information on each step and its associated tasks, please refer to the provided links.

**Connect with me on LinkedIn:** If you're interested in discussing this project further or exploring opportunities for collaboration, feel free to connect with me on [LinkedIn.](https://www.linkedin.com/in/eddieamaitum/) Let's connect and share insights!

