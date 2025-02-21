# USA-Housing-Data-Query-Times

## Description
The project aims to evaluate and analyze the USA Real Estate Dataset using Apache Spark to test and optimize query times for large-scale data analysis. The ultimate goal is to understand how Spark’s distributed framework can optimize the speed and scalability of queries on large datasets, providing insights into real estate trends, potential investment opportunities, and market dynamics. This project implements various queries using two API's capabilities to process and analyze the dataset in a distributed manner, comparing query times and performance across different operations.

- Resilient Distributed Dataset (RDD) API
- Dataframe API / Spark SQL

## Dataset Preview
This project uses a dataset with housing information, which comes from [USA Real Estate Dataset](https://www.kaggle.com/datasets/ahmedshahriarsakib/usa-real-estate-dataset). The CSV file contains 10 different columns:

1. brokered_by
2. status
3. price
4. bed
5. bath
6. acre_lot
7. street
8. city
9. state
10. zipcode
11. house_size
12. prev_sold_date

## Queries 

| Query | Description| 
|----------|----------|
| Query 1 | For each ZIP code, calculates the average and median prices of properties within them. |

## Stage 1

1. We convert the CSV file to Parquet format
2. We implement the queries and calculate the execution time for the following 3 cases:
- Map Reduce Queries - RDD API
- Spark SQL with CSV file as input
- Spark SQL with parquet file as input
3. Present the results of the execution times
