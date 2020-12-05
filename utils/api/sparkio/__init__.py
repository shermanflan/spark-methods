"""
Spark i/o utilities
"""


from utils import (
    SQL_HOST, SQL_DB, SQL_USER, SQL_PASSWORD
)

def load_dataset(session, from_path, as_format="parquet"):
    """
    Load partitioned parquet files to a Spark Dataframe. The from_path
    can be a local file path or a `wasb` path to an Azure blob folder.

    References:
        - https://docs.databricks.com/data/data-sources/azure/azure-storage.html

    :param session: the Spark session
    :param from_path: the parquet file location
    :param as_format: 
    :return: pyspark.sql.DataFrame
    """
    # DataFrameReader.format(args).option("key", "value").schema(args).load()
    return session.read.format(as_format).load(from_path)
    # return session.read.parquet(from_path)


def from_sql(session, table):

    # DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
    jdbc_url = f"jdbc:sqlserver://{SQL_HOST}:1433;database={SQL_DB}"

    # Write Option 1: Saving data to a JDBC source using save method
    return (session
            .read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table)
            .option("user", SQL_USER)
            .option("password", SQL_PASSWORD)
            .load())


def to_sql(df, table):
    """

    :param df:
    :return:
    """

    # DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
    jdbc_url = f"jdbc:sqlserver://{SQL_HOST}:1433;database={SQL_DB}"

    # Write Option 1: Saving data to a JDBC source using save method
    (df
     .write
     .mode('overwrite')
     .format("jdbc")
     .option("url", jdbc_url)
     .option("dbtable", table)
     .option("user", SQL_USER)
     .option("password", SQL_PASSWORD)
     .save())
