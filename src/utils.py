import json
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F, DataFrame


# get project directory
base_path = Path(__file__).resolve().parent.parent

# set up the configuration of logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s -- %(levelname)s : %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__file__)


def save_dataframe(dataframe: DataFrame, path: str) -> None:
    """
    Saves the dataframe to the given path by paritionned it by date
    and hour with the overwrite mode.

    Args:

        dataframe: DataFrame
            dataframe to be saved

        path: str
            path where the dataframe is saved
    """
    # get timestamp
    now = datetime.now()

    # get date from timestamp
    date = now.strftime('%Y-%m-%d')

    # get hour from timestamp
    hour = now.strftime('%H')

    (
        dataframe
        .withColumn('date', F.lit(date))
        .withColumn('hour', F.lit(hour))
        .coalesce(1)
        .write
        .partitionBy('date', 'hour')
        .csv(
            path=path,
            sep='|',
            encoding='utf-8',
            header=True,
            mode='overwrite'
        )
    )


def get_json_as_dict(path: str) -> dict:
    """
    Gets a json file content as a dictionnary.

    Args:

        path: str
            path where the json file is.

    Returns:
        _: dict
            content of the json file as a dictionnary
    """
    with open(path, 'r') as json_file:
        json_file_content_as_dict = json.load(json_file) or {}
    return json_file_content_as_dict
