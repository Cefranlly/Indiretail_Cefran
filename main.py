import sys
from pyspark.sql import *
from lib.logger.logger import Log4j
from lib.sparkconfig.utils import get_spark_app_config

from lib.utils.utils import load_parquet_to_df

# If you want to prove different config spark files
config_file = "spark.conf"
# App name
app_name = "IndiretailSpark"
data_dir = "data/"
products_file = data_dir + "products"
sales_file = data_dir + "sales"
stock_mov_file = data_dir + "stock_movements"

if __name__ == "__main__":
    conf = get_spark_app_config(config_file)

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: {} <filename>".format(app_name))
        sys.exit(-1)

    logger.info("Starting {}".format(app_name))

    products_df = load_parquet_to_df(spark, products_file)
    sales_df = load_parquet_to_df(spark, sales_file)
    stock_mov_df = load_parquet_to_df(spark, stock_mov_file)

    products_df.show(2)

    partitioned_survey_df = products_df.repartition(2)
    # count_df = count_by_country(partitioned_survey_df)
    # count_df.show()

    logger.info("Finished {}".format(app_name))
    spark.stop()
