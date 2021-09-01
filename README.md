# Pysaprk-AWS-Glue_ETL-Job
The Module performs the following Functions:  
* Displays the statistics of input dataset
* Reads data from csv files and stores the aggregated output in parquet format
* Counts the Number of records for each country/region and province/state
* Lists max Cases for each country/region and province/state
* Lists max Deaths for each country/region and province/state
* List max Recoveries for each country/region and province/state

# Script pyspark_etl_covid_job.py
This uses Pyspark dataframe for its ETL does the above mentioed fuctions.

# Script glue_pyspark_job.py
The implementation is specifically designed for AWS Glue environment. Can be used as a Glue Pyspark Job. The dataset being used was last updated on May 02, 2020. 
The Module performs the following Functions:
* Reads data from csv files stored on AWS S3
* Perfroms Extract, Transform, Load (ETL) operations. 
* Lists max Cases for each country/region and provice/state
* Lists max Deaths for each country/region and provice/state
* List max Recoveries for each country/region and provice/state
* stores the aggregated output in parquet format

