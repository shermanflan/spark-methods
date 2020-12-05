import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils import (
    STORAGE_ACCOUNT, STORAGE_KEY, APP_LOG_KEY
)
from utils.api.sparkio import load_dataset, to_sql, from_sql
from utils.config import log

logger = logging.getLogger(__name__)


def sql_expressions(session, df):

    logger.info(f"{df.columns}")

    # session.sql("CREATE DATABASE patient_model")
    # session.sql("USE patient_model;")
    # Create a managed table: persists after app terminates
    # df.write.saveAsTable(name="kinnser_patient")

    # Create a temporary view: expires when app terminates
    df.createOrReplaceTempView("kinnser_patient")  # session-scope
    # df.createOrReplaceGlobalTempView("kinnser_patient")  # cluster-scope

    (session
     .sql("""
        SELECT  AdmissionYear
                , State
                , Gender
                , ResidenceType
                , COUNT(*) AS Total
        FROM    kinnser_patient
        WHERE   AdmissionYear = 2020
        GROUP BY AdmissionYear
                , State
                , Gender
                , ResidenceType
        ORDER BY State
                , Total DESC
     """)
     .show(10, truncate=False)
     )

    patient_2019 = (df.select('AdmissionYear')
                    .where(col('AdmissionYear') == 2019))
    patient_2019.createOrReplaceTempView("kinnser_patient_2019")  # session-scope

    (session
     .sql("""
        SELECT  AdmissionYear
                , COUNT(*) AS Total
        FROM    kinnser_patient_2019
        GROUP BY AdmissionYear
        ORDER BY Total DESC
     """)
     .show(10, truncate=False)
     )

    # spark.catalog.dropTempView("kinnser_patient")
    # spark.catalog.dropGlobalTempView("kinnser_patient")

    # Equivalent using DataFrame API.
    # (df
    #  .select('AdmissionYear', 'State', 'Gender', 'ResidenceType')
    #  .where(col('AdmissionYear') == 2020)
    #  .groupBy('AdmissionYear', 'State', 'Gender', 'ResidenceType')
    #  .count()
    #  .orderBy(['State', 'count'], ascending=[True, False])
    #  .show(10, truncate=False)
    # )


if __name__ == '__main__':

    logger.info('Creating spark session')

    spark = (SparkSession
             .builder
             .appName(APP_LOG_KEY)
             # .config(
             #    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
             #    STORAGE_KEY)
             .getOrCreate())
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
        STORAGE_KEY)

    logger.info('Reading parquet dataset')

    kinnser_patients_path = '/home/condesa1931/personal/github/py-methods/parquet/jupyter/data/KinnsrBIBaseData'
    # kinnser_patients_path = 'wasbs://enterprisedata@airflowstoragesandbox.blob.core.windows.net/KinnsrBIBaseData'
    kinnser_patients_df = load_dataset(spark, kinnser_patients_path)

    # sql_expressions(spark, kinnser_patients_df)
    df1 = (kinnser_patients_df
           .select('AdmissionYear', 'State', 'Gender', 'ResidenceType')
           .where(col('AdmissionYear') == 2020)
    )
    df1.show(10, truncate=False)

    logger.info('Writing dataset to SQL')
    to_sql(df1, table="dbo.KinnserPatient")

    logger.info('Reading dataset from SQL')

    (from_sql(spark, table="dbo.KinnserPatient")
     .select('State')
     .groupBy('State')
     .count()
     .show(10))

    logger.info('Processing complete')
