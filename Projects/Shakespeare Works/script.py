from pyspark import SparkContext
from nltk.corpus import stopwords

# Initialize SparkContext
sc = SparkContext()

# Create a baseRDD from the file path
baseRDD = sc.textFile("WorksOfShakespeare.txt")

# Split the lines of baseRDD into words
splitRDD = baseRDD.flatMap(lambda x: x.split(' '))

# Count the total number of words
print("Total number of words in splitRDD:", splitRDD.count())

stop_words = set(stopwords.words('english'))

# Convert the words in lower case and remove stop words from stop_words
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1 
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

# Swap the keys and values 
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies
for word in resultRDD_swap_sort.take(10):
	print("{} has {} counts". format(word[1], word[0]))