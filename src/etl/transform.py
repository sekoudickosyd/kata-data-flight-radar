from pyspark.sql import SparkSession, functions as F, DataFrame, Window

from utils import get_json_as_dict, save_dataframe, logger, base_path


class Transform:

    def __init__(self, spark_session: SparkSession):
        # sparksession used as an entry point of the application
        self.spark_session = spark_session

        # get a transform configuration
        self.transform_conf = (
            get_json_as_dict(path=base_path / 'configuration' / 'etl.json')
            .get('transform', None)
        )

        # get input data path
        self.input_path = str(base_path / self.transform_conf['input_path'])

        # get transform output_path
        self.output_path = base_path / self.transform_conf['output_path']

        # load the last partition of extracted data
        self.dataframe = (
            self.spark_session.read.csv(
                path=self.input_path,
                sep='|',
                header=True
            )
            .replace('N/A', None)
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
        )

    def get_airline_with_most_flights(self) -> DataFrame:
        """
        Gets the airline with the most flights

        Returns:
            _: DataFrame
                dataframe containing the columns airline_icao,
                airline_name and flights_number
        """
        return (
            self.dataframe
            .groupBy('airline_icao', 'airline_name')
            .agg(F.countDistinct('id').alias('flights_number'))
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.orderBy(F.desc_nulls_last('flights_number'))
                )
            )
            .filter(F.col('rank') == 1)
            .select(
                'airline_icao',
                'airline_name',
                'flights_number'
            )
        )

    def get_airline_with_most_active_regional_flights_by_continent(self) -> DataFrame:
        """
        Gets for each continent, the airline with the most active
        regional flights

        Returns:
            _: DataFrame
                dataframe containing the columns continent, airline_icao,
                airline_name and flights_number
        """
        return (
            self.dataframe
            .filter(
                F.col('origin_airport_continent') == F.col('destination_airport_continent')
            )
            .groupBy('origin_airport_continent', 'airline_icao', 'airline_name')
            .agg(F.countDistinct('id').alias('flights_number'))
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.partitionBy('origin_airport_continent')
                    .orderBy(F.desc_nulls_last('flights_number'))
                )
            )
            .filter(F.col('rank') == 1)
            .select(
                F.col('origin_airport_continent').alias('continent'),
                'airline_icao',
                'airline_name',
                'flights_number'
            )
        )

    def get_longest_flight(self) -> DataFrame:
        """
        Gets the flight with the longest distance.

        Returns:
            _: DataFrame
                dataframe containing the columns id, aircraft_model,
                airline_name, origin_airport_name,  destination_airport_name
                and distance_origine_destination_airports_km
        """
        return (
            self.dataframe
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.orderBy(F.desc_nulls_last('distance_origine_destination_airports_km'))
                )
            )
            .filter(F.col('rank') == 1)
            .select(
                'id',
                'aircraft_model',
                'airline_name',
                'origin_airport_name',
                'destination_airport_name',
                'distance_origine_destination_airports_km'
            )
        )

    def get_average_flight_length_by_continent(self) -> DataFrame:
        """
        Gets for each continent, the average of the distance of the
        flights

        Returns:
            _: DataFrame
                dataframe containing the columns continent and
                average_flight_length_km
        """
        return (
            self.dataframe
            .filter(
                F.col('origin_airport_continent') == F.col('destination_airport_continent')
            )
            .groupBy('origin_airport_continent')
            .agg(
                F.round(
                    F.avg('distance_origine_destination_airports_km'), 2
                ).alias('average_flight_length_km')
            )
            .select(
                F.col('origin_airport_continent').alias('continent'),
                'average_flight_length_km'
            )
        )

    def get_aircraft_manufacturer_with_most_active_flights(self) -> DataFrame:
        """
        Gets the aircraft manufacturer with the most active flights

        Returns:
            _: DataFrame
                dataframe containing the columns aircraft_manufacturer and
                flights_number
        """
        return (
            self.dataframe
            .withColumn(
                'aircraft_manufacturer',
                F.split('aircraft_model', ' ').getItem(0)
            )
            .groupBy('aircraft_manufacturer')
            .agg(F.countDistinct('id').alias('flights_number'))
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.orderBy(F.desc_nulls_last('flights_number'))
                )
            )
            .filter(F.col('rank') == 1)
            .select(
                'aircraft_manufacturer',
                'flights_number'
            )
        )

    def run(self) -> None:
        """Runs the transform step to calculate the indicators"""

        # methods of the indicators to be calculated
        methods = [
            attribute
            for attribute in dir(Transform)
            if callable(getattr(Transform, attribute)) and attribute.startswith('get_')
        ]

        for method in methods:
            # get indicator name
            indicator_name = method.split('get_')[-1]

            # get the output path of the calculated indicator
            path = self.output_path / indicator_name

            # calculate the indicator
            indicator = getattr(self, method)()

            # save the calculated indicator
            save_dataframe(dataframe=indicator, path=str(path))

            indicator_name = indicator_name.replace('_', ' ')

            logger.info(f"Indicator {indicator_name} saved")
