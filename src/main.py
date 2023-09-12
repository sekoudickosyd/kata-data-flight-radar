from pyspark.sql import SparkSession

from src.etl.extract import Extract
from src.etl.transform import Transform
from src.etl.load import Load
from utils import logger

# sparksession used as an entry point of the application
spark_session = (
    SparkSession.builder
    .appName('ETL')
    .getOrCreate()
)

# set log level off
spark_session.sparkContext.setLogLevel('OFF')


# ==============================
#           Extract
# ==============================

logger.info('Start extracting ....')

# instantiate Extract
extract = Extract(spark_session=spark_session, numbers=100)

# launch the extract step
extract.run()


# ================================
#           Transform
# ================================

logger.info('Start transforming ....')

# instantiate Transform
transform = Transform(spark_session=spark_session)

# launch the transform step
transform.run()


# ================================
#           Load
# ================================

logger.info('Start loading ....')

# instantiate Load
load = Load(spark_session=spark_session)

# launch the load step
load.run()
