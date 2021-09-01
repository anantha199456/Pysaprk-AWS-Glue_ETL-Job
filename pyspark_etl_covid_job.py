# Displays the statistics of input dataset
# Reads data from csv files and stores the aggregated output in parquet format
# Counts the Number of records for each country/region and province/state
# Lists max Cases for each country/region and province/state
# Lists max Deaths for each country/region and province/state
# List max Recoveries for each country/region and province/state
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import boto3


def get_spark_configuration(bucket, key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    configfile = yaml.safe_load(response["Body"])
    return configfile


def display_dataset_info(csv_df):
    csv_df.printSchema()
    csv_df.show(20)
    csv_df.describe().show()  # data statistics summary of attributes in the given file.


def data_cleaning(raw_df):
    dropped_df = raw_df.na.drop()  # if you want to remove the whole row which contains null value.
    # print(dropped_df.count())
    corona_df = raw_df.dropna(thresh=4)
    cleansed_data_df = corona_df.fillna(value='Province/State', subset='Province/State')
    return cleansed_data_df


def count_records_by_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region').count().orderBy('count', ascending=False)


def count_records_by_province_state(dataframe_df):
    return dataframe_df.groupBy('Province/State').count().orderBy('count', ascending=False)


def confirmed_cases_by_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region').max('Confirmed').select('Country/Region',
                                                                          f.col('max(Confirmed)')
                                                                          .alias("Most_Cases")) \
        .orderBy("max(Confirmed)", ascending=False)


def confirmed_cases_by_province_state(dataframe_df):
    return dataframe_df.groupBy('Province/State').max('Confirmed').select('Country/Region',
                                                                          f.col('max(Confirmed)')
                                                                          .alias("Most_Cases")) \
        .orderBy("max(Confirmed)", ascending=False)


def death_cases_by_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region').max('Deaths').select('Country/Region',
                                                                       f.col('max(Deaths)').alias("Most_Deaths")) \
        .orderBy("max(Deaths)", ascending=False)


def recovery_cases_by_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region').max('Recovered').select('Country/Region',
                                                                          f.col('max(Recovered)').alias(
                                                                              "Most_Recoveries")) \
        .orderBy("max(Recovered)", ascending=False)


def recovery_cases_by_province_state_and_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region', 'Province/State').max('Recovered').select('Country/Region',
                                                                                            'Province/State',
                                                                                            f.col(
                                                                                                'max(Recovered)').alias(
                                                                                                "Most_Recoveries")) \
        .orderBy("max(Recovered)", ascending=False)


def death_cases_by_province_state_and_country_region(dataframe_df):
    return dataframe_df.groupBy('Country/Region', 'Province/State').max('Deaths').select('Country/Region', 'Province'
                                                                                                           '/State',
                                                                                         f.col('max(Deaths)').alias(
                                                                                             "Most_Deaths")) \
        .orderBy("max(Deaths)", ascending=False)


def save_data_in_parquet(dataframes, filename):
    dataframes.coalesce(1).write.mode("overwrite").parquet(filename)


def display(df):
    df.show(50)


if __name__ == '__main__':
    # create spark session
    spark = SparkSession.builder.appName("Covid-demo").getOrCreate()
    config_file = get_spark_configuration('saama-anantha-bootcamp', 'covid-config.yaml')

    covid_dataset = config_file['s3_location_of_dataset']
    # read the csv file.
    corona_raw_df = spark.read.csv(covid_dataset, inferSchema=True, header=True)

    # display dataset statistics, schema and top rows
    display_dataset_info(corona_raw_df)
    # data_cleaning - drop rows wit n/a values otherwise fill with custom value.
    # Province/State column is filled with a custom value of 'Province/State' for all rows missing this value
    corona_filtered_df = data_cleaning(corona_raw_df)

    # count of records by country or region and save the output in parquet format.
    output_file = 'records_count_by_country_region'
    dataframe = count_records_by_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # count of records by province or state and save the output in parquet format.
    output_file = 'records_count_by_province_state'
    dataframe = count_records_by_province_state(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Confirmed cases by country or region and save the output in parquet format.
    output_file = 'confirmed_cases_by_country_region'
    dataframe = confirmed_cases_by_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Confirmed cases by province or state and save the output in parquet format.
    output_file = 'confirmed_cases_by_province_state'
    dataframe = confirmed_cases_by_province_state(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Deaths by country or region and save the output in parquet format.
    output_file = 'death_cases_by_country_region'
    dataframe = death_cases_by_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Deaths by province or state of a country or region and save the output in parquet format.
    output_file = 'death_cases_by_province_state_and_country_region'
    dataframe = death_cases_by_province_state_and_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Recoveries by country or region and save the output in parquet format.
    output_file = 'recover_cases_by_country_region'
    dataframe = recovery_cases_by_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)

    # most Recoveries by province or state of a country or region and save the output in parquet format.
    output_file = 'recovery_cases_by_province_state_and_country_region'
    dataframe = recovery_cases_by_province_state_and_country_region(corona_filtered_df)
    save_data_in_parquet(dataframe, output_file)
    display(dataframe)
