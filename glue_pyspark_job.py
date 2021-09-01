"""
The implementation is specifically
designed for AWS Glue environment. Can be used as a Glue Pyspark Job.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# catalog: database and table names, s3 output bucket
database_name = 'glue_etl_db'
table = 'glue_etl_db'
s3_bucket = 's3 bucket path'

# EXTRACT
# creating datasource using the catalog table
datasource0 = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table,
                                                            transformation_ctx="datasource0")
# converting from Glue DynamicFrame to Spark Dataframe
dataframe = datasource0.toDF()

# TRANSFORM
# dropping the last update column
dataframe.dropna('Last Update')
# dropping rows if a row contains more than 4 null values
dataframe.dropna(tresh=4)
# replacing the missing value in Province/State column and populating with a default value
cleaned_df = dataframe.fillna(value='na_province/state', subset='Province/State')
# Grouping the records by Province/State and Country/Region column, aggregating with max(Confirmed) column
# and sorting them in descending order of max of Confirmed cases.
most_cases_province_state_df = cleaned_df.groupBy('Country/Region', 'Province/State').max('Confirmed').select(
    'Country/Region', 'Province/State', f.col('max(Confirmed)').alias("Most_Confirmed")).orderBy("max(Confirmed)",
                                                                                                 ascending=False)
# Grouping the records by Province/State and Country/Region column, aggregating with max(Deaths) column
# and sorting them in descending order of max of Deaths.
most_deaths_province_state_df = cleaned_df.groupBy('Country/Region', 'Province/State').max('Deaths').select(
    'Country/Region', 'Province/State', f.col('max(Deaths)').alias("Most_Deaths")).orderBy("max(Deaths)",
                                                                                           ascending=False)
# Grouping the records by Province/State and Country/Region column, aggregating with max(Recovered) column
# and sorting them in descending order of max of Recovered.
most_recovered_province_state_df = cleaned_df.groupBy('Country/Region', 'Province/State').max('Recovered').select(
    'Country/Region', 'Province/State', f.col('max(Recovered)').alias("Most_Recovered")).orderBy("max(Recovered)",
                                                                                                 ascending=False)

# transforming Spark Dataframes back to Glue DynamicFrames
transformation1 = DynamicFrame.fromDF(most_cases_province_state_df, glueContext, 'transformation1')
transformation2 = DynamicFrame.fromDF(most_deaths_province_state_df, glueContext, 'transformation2')
transformation3 = DynamicFrame.fromDF(most_recovered_province_state_df, glueContext, 'transformation3')

# LOAD
# Storing the data on s3 specified path in parquet format
datasink1 = glueContext.write_dynamic_frame.from_catalog(frame=transformation1, connection_type="s3",
                                                         connection_options={
                                                             "path": s3_bucket + '/most-cases'},
                                                         format="parquet", transformation_ctx="datasink1")
datasink2 = glueContext.write_dynamic_frame.from_catalog(frame=transformation2, connection_type="s3",
                                                         connection_options={
                                                             "path": s3_bucket + '/most-deaths'},
                                                         format="parquet", transformation_ctx="datasink2")
datasink3 = glueContext.write_dynamic_frame.from_catalog(frame=transformation3, connection_type="s3",
                                                         connection_options={
                                                             "path": s3_bucket + '/most-recoveries'},
                                                         format="parquet", transformation_ctx="datasink3")
job.commit()
