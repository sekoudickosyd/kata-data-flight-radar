from pyspark.sql import SparkSession, functions as F, Window

from utils import get_json_as_dict, logger, base_path


class Load:

    indicators = [
        'airline_with_most_flights',
        'airline_with_most_active_regional_flights_by_continent',
        'longest_flight',
        'average_flight_length_by_continent',
        'aircraft_manufacturer_with_most_active_flights'
    ]

    def __init__(self, spark_session: SparkSession):

        # sparksession used as an entry point of the application
        self.spark_session = spark_session

        # get a load configuration
        self.load_conf = (
            get_json_as_dict(path=base_path / 'configuration' / 'etl.json')
            .get('load', None)
        )

        # input path of transformed data
        self.input_path = base_path / self.load_conf['input_path']

    def run(self) -> None:
        """Runs load step"""

        for indicator in self.indicators:

            try:
                # get path of the indicator
                path = self.input_path / indicator

                print(f"{'=' * (len(indicator) + 10)}")
                print(f"\t{indicator.replace('_', ' ')}")
                print(f"{'=' * (len(indicator) + 10)}")

                # get the last partition of the calculated indicator as
                # a dataframe
                dataframe = (
                    self.spark_session.read.csv(
                        path=str(path),
                        sep='|',
                        header=True
                    )
                    .alias('data')
                    .withColumn(
                        'rank',
                        F.dense_rank().over(
                            Window.orderBy(
                                F.desc_nulls_last('date'),
                                F.desc_nulls_last('hour')
                            )
                        )
                    )
                    .filter(F.col('rank') == 1)
                    .select('data.*')
                )

                # print the dataframe
                dataframe.show(truncate=False)

            except Exception as e:
                logger.error(e)
                continue
