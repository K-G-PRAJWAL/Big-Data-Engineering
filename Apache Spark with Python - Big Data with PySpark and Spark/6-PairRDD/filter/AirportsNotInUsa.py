import sys
sys.path.insert(0, '.')


from pyspark import SparkContext, SparkConf
from commons.Utils import Utils


if __name__ == "__main__":
    '''
    Create a Spark program to read the airport data from in/airports.text;
    generate a pair RDD with airport name being the key and country name being the value.
    Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    ("Kamloops", "Canada")
    ("Wewak Intl", "Papua New Guinea")
    ...

    '''

    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    airportsRDD = sc.textFile("inputs/airports.text")

    airportPairRDD = airportsRDD.map(lambda line:
                                     (Utils.COMMA_DELIMITER.split(line)[1],
                                      Utils.COMMA_DELIMITER.split(line)[3]))
    airportsNotInUSA = airportPairRDD.filter(
        lambda keyValue: keyValue[1] != "\"United States\"")

    airportsNotInUSA.saveAsTextFile(
        "outputs/airports_not_in_usa_pair_rdd.text")
