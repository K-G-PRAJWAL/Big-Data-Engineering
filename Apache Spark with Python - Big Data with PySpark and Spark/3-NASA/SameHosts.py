from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    conf = SparkConf().setAppName("sameHosts").setMaster("local[1]")
    sc = SparkContext(conf=conf)

    julyFirstLogs = sc.textFile("inputs/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("inputs/nasa_19950801.tsv")

    julyFirstHosts = julyFirstLogs.map(lambda line: line.split("\t")[0])
    augustFirstHosts = augustFirstLogs.map(lambda line: line.split("\t")[0])

    intersection = julyFirstHosts.intersection(augustFirstHosts)

    cleanedHostIntersection = intersection.filter(lambda host: host != "host")
    cleanedHostIntersection.saveAsTextFile("outputs/nasa_logs_same_hosts.csv")
