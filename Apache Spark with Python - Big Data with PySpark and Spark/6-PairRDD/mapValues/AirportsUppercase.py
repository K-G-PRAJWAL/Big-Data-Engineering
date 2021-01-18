import sys
sys.path.insert(0, '.')

from pyspark import SparkContext, SparkConf
from commons.Utils import Utils


if __name__ == "__main__":
    '''
      Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
      being the key and country name being the value. Then convert the country name to uppercase and
      output the pair RDD to out/airports_uppercase.text

      Each row of the input file contains the following columns:

      Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
      ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

      Sample output:

      ("Kamloops", "CANADA")
      ("Wewak Intl", "PAPUA NEW GUINEA")
      ...

      '''
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    airportsRDD = sc.textFile("inputs/airports.text")

    airportPairRDD = airportsRDD.map(lambda line:
                                     (Utils.COMMA_DELIMITER.split(line)[1],
                                      Utils.COMMA_DELIMITER.split(line)[3]))

    upperCase = airportPairRDD.mapValues(
        lambda countryName: countryName.upper())

    upperCase.saveAsTextFile("outputs/airports_uppercase.text")
