from pyspark.sql import SparkSession
from pyspark.sql.types import *

# See https://docs.databricks.com/data/data-sources/azure/azure-storage.html
if __name__ == "__main__":

    #create a SparkSession
    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        .config("fs.azure.account.key.<storage account>.blob.core.windows.net",
                "<storage key>")
        .getOrCreate())

    # Use the DataFrameReader interface to read a parquet file
    sf_fire_file = "wasbs://enterprisedata@airflowstoragesandbox.blob.core.windows.net/parquet"
    fire_df = spark.read.parquet(sf_fire_file)

    fire_df.select("PatientKey", "LastName", "FirstName").show(n=5)