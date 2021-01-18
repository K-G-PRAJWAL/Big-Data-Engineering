from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")

    # Spark Context
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Load input
    lines = sc.textFile("inputs/word_count.text")

    # Split the sentences into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count occurrence of each word
    wordCounts = words.countByValue()

    # Print the count
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))
