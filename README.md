# Udacity STEDI Human Balance Analytics
Data Pipeline built in AWS that ingests data through S3, transforms the data, and loads the data for analytics and Machine Learning

##Pipeline Architecture
Landing Zone- Raw unstructured data
Trusted Zone- AWS Glue PySpark filters and queries the data to exclude certain users
Curated Zone- Final datasets are joined and ready for Analytics

##Tech Utilized
AWS S3- ingested raw data
AWS Glue- ETL and Pyspark
AWS Athena- SQL

