

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# See https://docs.databricks.com/data/data-sources/azure/azure-storage.html
if __name__ == "__main__":

    #create a SparkSession
    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        # .config("fs.azure.account.key.<account>.blob.core.windows.net",
        #         "<account key>")
        .getOrCreate())

    # Use the DataFrameReader interface to read a parquet file
    sf_fire_file = "/home/condesa1931/personal/github/py-methods/parquet/jupyter/data/KinnsrBIBaseData"
    # sf_fire_file = "wasbs://enterprisedata@airflowstoragesandbox.blob.core.windows.net/parquet"
    fire_df = spark.read.parquet(sf_fire_file)

    # grouped_df = (fire_df
    #               .select("PatientKey", "LastName", "FirstName", "Gender", "State")
    #               .where(col('State') == 'TN'))
    # grouped_df.show(n=5)

    # new_fire_df = fire_df.withColumnRenamed('Gender', 'Sex')
    # grouped_df = (new_fire_df
    #               .select("Sex", "City")
    #               .where(col('State') == 'TN')
    #               .distinct()
    #               .show(n=10)
    #               )

    # grouped_df = (fire_df
    #               .withColumn("IncidentDate", to_timestamp(col("AdmissionDate"), "MM/dd/yyyy"))
    #               # .drop("AdmissionDate")
    #               .select("PatientKey", "LastName", "FirstName",
    #                       "Gender", "State", "AdmissionDate",
    #                       "IncidentDate")
    #               .where(col('State') == 'TN')
    #               )
    # grouped_df.show(n=5)

    # grouped_df = (fire_df
    #               .select("PatientKey", "LastName", "FirstName",
    #                       "Gender", "State", "AdmissionDate",
    #                       year("AdmissionDate"))
    #               .where(col('Gender') == 'FEMALE')
    #               .orderBy(col('PatientKey'))
    #               )
    # grouped_df.show(n=5)

    grouped_df = (fire_df
                  .select("State")
                  .groupBy(col('State'))
                  .count()
                  .orderBy('count', ascending=False)
                  )
    grouped_df.show(n=10)
