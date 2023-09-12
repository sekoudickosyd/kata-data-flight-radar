from typing import List, Tuple

from FlightRadar24 import FlightRadar24API
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from utils import get_json_as_dict, save_dataframe, logger, base_path

# get an instance of FlightRadar24API
fr_api = FlightRadar24API(...)


class Extract:

    def __init__(self, spark_session: SparkSession, numbers: int = None):

        # sparksession used as an entry point of the application
        self.spark_session = spark_session

        # get an extract configuration of the tables
        self.extract_tables_conf = (
            get_json_as_dict(path=base_path / 'configuration' / 'etl.json')
            .get('extract', None)
        )

        # define the number of rows to get via call api
        self.numbers = numbers

    def flight_data(self, columns: List[str]) -> List[List]:
        """
        Gets flights data from FlightRadar24API

        Args:

            columns: List[str]
                columns to be extracted

        Returns:
            data: List[List]
                each element of the list represents a flight info
        """

        # columns to be calculated from the attributes of the flight object
        calculated_columns = [
            'distance_origine_destination_airports_km',
            'origin_airport_continent',
            'destination_airport_continent'
        ]

        # define a list which will contain a list representing a flight info
        data = []

        # initialize a counter
        i = 0

        for flight in fr_api.get_flights():

            try:
                # add details to flight info
                flight_details = fr_api.get_flight_details(flight)
                flight.set_flight_details(flight_details)

                # check if all the needed attributes are not 'N/A'
                if all(
                        getattr(flight, col) != 'N/A'
                        for col in columns
                        if col not in calculated_columns
                ):
                    # get origin and destination airports objects
                    origin_airport = fr_api.get_airport(code=flight.origin_airport_iata)
                    destination_airport = fr_api.get_airport(code=flight.destination_airport_iata)

                    # calculate the distance between origin and destination airports
                    distance_origine_destination_airports = round(
                        origin_airport.get_distance_from(destination_airport), 2
                    )

                    # get the continents of origin and destination airports
                    origin_airport_continent = flight.origin_airport_timezone_name.split('/')[0]
                    destination_airport_continent = flight.destination_airport_timezone_name.split('/')[0]

                    # add flight info list to data list
                    data.append(
                        [
                            getattr(flight, col) for col in columns if col not in calculated_columns
                        ]
                        + [
                            origin_airport_continent,
                            destination_airport_continent,
                            distance_origine_destination_airports
                        ]
                    )

                    logger.info(f'Flight added : {flight}')

                    # increment the counter
                    i += 1

            except Exception as e:
                logger.error(e)
                continue

            if self.numbers and i == self.numbers:
                break

        return data

    @staticmethod
    def schema(columns: List[str]) -> StructType:
        """
        Defines pyspark schema from columns
        """
        return StructType([
            StructField(col, StringType(), nullable=True)
            for col in columns
        ])

    def dataframe(self, data: List[Tuple], schema: StructType) -> DataFrame:
        """
        Creates a dataframe from data and schema and returns it
        """
        return self.spark_session.createDataFrame(data=data, schema=schema)

    def run(self) -> None:
        """Runs the extract of all the tables"""

        for extract_table_conf in self.extract_tables_conf:
            # get the name of the table to be extract
            table_name = extract_table_conf['table_name']

            # get the columns of the table
            columns = extract_table_conf['columns']

            # get the data of the table via call api
            data = getattr(self, f'{table_name}_data')(columns=columns)

            # get the schema of the table
            schema = self.schema(columns)

            # create the dataframe representing the table
            dataframe = self.dataframe(data=data, schema=schema)

            # get the path where the dataframe will be saved
            path = base_path / extract_table_conf['output_path']

            # save the extracted table
            save_dataframe(dataframe=dataframe, path=str(path))
