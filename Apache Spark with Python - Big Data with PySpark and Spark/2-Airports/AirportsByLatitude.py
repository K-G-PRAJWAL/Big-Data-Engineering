import sys
sys.path.insert(0, '.')

from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])


if __name__ == "__main__":
    '''
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    ...
    '''
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    airports = sc.textFile("inputs/airports.text")

    airportsInUSA = airports.filter(lambda line: float(
        Utils.COMMA_DELIMITER.split(line)[6]) > 40)

    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    airportsNameAndCityNames.saveAsTextFile("outputs/airports_by_latitude.text")
