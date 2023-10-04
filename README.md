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
