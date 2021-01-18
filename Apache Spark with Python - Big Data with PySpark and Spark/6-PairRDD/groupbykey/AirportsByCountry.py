import sys
sys.path.insert(0, '.')

from commons.Utils import Utils
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text,
    output the the list of the names of the airports located in each country.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
    "Papua New Guinea",  ["Goroka", "Madang", ...]
    ...

    '''
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("inputs/airports.text")

    countryAndAirportNameAndPair = lines.map(lambda airport:
                                             (Utils.COMMA_DELIMITER.split(airport)[3],
                                              Utils.COMMA_DELIMITER.split(airport)[1]))

    airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for country, airportName in airportsByCountry.collectAsMap().items():
        print("{}: {}".format(country, list(airportName)))
