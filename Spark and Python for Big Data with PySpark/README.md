# Spark and Python for Big Data with PySpark

### Spark Installation

- Local VirtualBox Setup
- AWS EC2 PySpark setup
- Databricks setup
- AWS EMR Cluster setup

---

### Spark DataFrame

- Hold data in row and column format
- RDD syntax(old) -> DataFrame(new)
- Input adn output data from wide variety of sources
- Provides DataFrame MLlib API

###### DataFrame Basics

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('people.json')

df.printSchema()
df.columns
df.describe().show()
df.show()

from pyspark.sql.types import StructField, StringType, IntegerType, StructType
data_schema = [StructField('age', IntegerType(), True), StructField('name', StringType(), True)]
final_struc = StructType(fields=data_schema)
df = spark.read.json('people.json', schema=final_struc)
df.printSchema()

df.select('age').show()
df.head(2)
df.select(['age', 'name']).show()
df.withColumn('new_age', df['age']).show()
df.withColumn('double_age', df['age']*2).show()
df.withColumnRenamed('age', 'my_new_age').show()

df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people")
results.show()
new_results = spark.sql("SELECT * FROM people where age=30")
new_results.show()
```

```python
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.printSchema()
df.show()
df.head(3)[0]

df.filter("Close < 500").show()
df.filter("Close < 500").select(['Open', 'Close']).show()
df.filter(df['Close']<500).select('Volume').show()
df.filter((df['Close']<200) & (df['Open'])>200).show()
df.filter((df['Close']<200) & ~(df['Open'])>200).show()
result = df.filter(df['Low']==197.16).collect()
result.asDict()['Volume']
```

###### Groupby and Aggregate functions

```python
df.spark.read.csv('sales_info.csv', inferSchema=True, header=True)
df.show()

df.groupBy('Company')
df.groupBy('Company').mean().show()
df.groupBy('Company').sum().show()
df.groupBy('Company').max().show()
df.groupBy('Company').min().show()
df.groupBy('Company').count().show()

df.agg({'Sales':'sum'}).show()
df.agg({'Sales':'max'}).show()

group_data = df.groupBy('Company')
group_data.agg({'Sales':'max'})
```

```python
from pyspark.sql.functions import countDistinct, avg, stddev
df.select(countDistinct('Sales')).show()
df.select(avg('Sales').alias('Average Sales')).show()
df.select(stddev('Sales')).show()

from pyspark.sql.functions import format_number
sales_std = df.select(stddev('Sales'))
sales_std.select(format_number('std', 2).alias('std')).show()

df.orderBy('Sales').show()
df.orderBy(df['Sales'].desc()).show()
```

###### Handling Missing Data

```python
df = spark.read.csv("ContainsNull.csv",header=True,inferSchema=True)
df.show()

df.na.drop().show()
df.na.drop(thresh=2).show()
df.na.drop(how='any').show()
df.na.drop(how='all').show()

df.printSchema()
df.na.fill('Val').show()
df.na.fill(0).show()
df.na.fill('No Name', subset=['Name']).show()

from pyspark.sql.functions import mean
mean_val = df.select(mean(df['Sales'])).collect()
mean_val[0][0]
df.na.fill(mean_sales,["Sales"]).show()

df.na.fill(df.select(mean(df['Sales'])).collect()[0][0],['Sales']).show()
```

###### Dates and timestamps

```python
df = spark.read.csv("appl_stock.csv",header=True,inferSchema=True)
df.show()

from pyspark.sql.functions import format_number, dayofmonth, hour, dayofyear, month, year, weekofyear, date_format
df.select(dayofmonth(df['Date'])).show()
df.select(hour(df['Date'])).show()
df.select(dayofyear(df['Date'])).show()
df.select(month(df['Date'])).show()
df.select(year(df['Date'])).show()

newdf = df.withColumn("Year",year(df['Date']))
result = newdf.groupBy("Year").mean()[['avg(Year)','avg(Close)']]
result = result.withColumnRenamed("avg(Close)","Average closing price per year")
result = result.select('Year',format_number('Average closing price per year',2).alias("Mean Close")).show()
```

---

### Machine Learning

###### Linear Regression

```python
from pyspark.ml.regression import LinearRegression
training = spark.read.format('libsvm').load('sample_linear_regression_data.txt')
training.show()
lr = LinearRegression(featuresCol='features', labelCol='label', predictionCol='prediction')
lr_model = lr.fit(training)
lr_model.coefficients
lr_model.intercept
training_summary = lr_model.summary
training_summary.rootMeanSquaredError
training_summary.totalIterations
training_summary.objectiveHistory
training_summary.r2

all_data = spark.read.format('libsvm').load('sample_linear_regression_data.txt')
train_data, test_data = all_data.randomSplit([0.7, 0.3])
correct_model = lr.fit(train_data)
test_results = correct_model.evaluate(test_data)
test_results.residuals.show()
test_results.rootMeanSquaredError

unlabelled_data = test_data.select('features')
predictions = correct_model.transform(unlabelled_data)
predictions.show()
```

```python
spark = SparkSession.builder.appName('lr_example').getOrCreate()
from pyspark.ml.regression import LinearRegression
data = spark.read.csv("Ecommerce_Customers.csv", inferSchema=True, header=True)
data.printSchema()

for item in data.head():
    print(item)

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
    inputCols = ["Avg Session Length", "Time on App", 
               "Time on Website", 'Length of Membership'],
    outputCol = "features")
output = assembler.transform(data)
output.select("features").show()
final_data = output.select("features", 'Yearly Amount Spent')
train_data, test_data = final_data.randomSplit([0.7, 0.3])
train_data.describe().show()
test_data.describe().show()
lr = LinearRegression(labelCol='Year Amount Spent')
lr_model = lr.fit(train_data)
lr_model.coefficients
lr_model.intercept
test_results = lr_model.evaluate(test_data)
test_results.residuals.show()
test_results.rootMeanSquaredError
test_results.r2
unlabeled_data = test_data.select('features')
unlabeled_data.show()
predictions = lr_model.transform(unlabeled_data)
predictions.show()
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Consulting%20Project/Consulting%20Project%20-%20Linear%20Regression)

---

###### Logistic Regression

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('logregdoc').getOrCreate()

from pyspark.ml.classification import LogisticRegression
training = spark.read.format("libsvm").load("sample_libsvm_data.txt")
lr = LogisticRegression()
lrModel = lr.fit(training)
trainingSummary = lrModel.summary
trainingSummary.predictions.show()

from pyspark.mllib.evaluation import MulticlassMetrics
lrModel.evaluate(training)
predictionAndLabels = lrModel.evaluate(training)
predictionAndLabels.predictions.show()

predictionAndLabels = predictionAndLabels.predictions.select('label','prediction')
predictionAndLabels.show()

from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label')
evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label',
                                             metricName='accuracy')
acc = evaluator.evaluate(predictionAndLabels)
acc
```

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('myproj').getOrCreate()
data = spark.read.csv('titanic.csv',inferSchema=True,header=True)
data.printSchema()

data.columns
my_cols = data.select(['Survived',
 'Pclass',
 'Sex',
 'Age',
 'SibSp',
 'Parch',
 'Fare',
 'Embarked'])
my_final_data = my_cols.na.drop()

from pyspark.ml.feature import (VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer)
gender_indexer = StringIndexer(inputCol='Sex',outputCol='SexIndex')
gender_encoder = OneHotEncoder(inputCol='SexIndex',outputCol='SexVec')
embark_indexer = StringIndexer(inputCol='Embarked',outputCol='EmbarkIndex')
embark_encoder = OneHotEncoder(inputCol='EmbarkIndex',outputCol='EmbarkVec')
assembler = VectorAssembler(inputCols=['Pclass',
 'SexVec',
 'Age',
 'SibSp',
 'Parch',
 'Fare',
 'EmbarkVec'],outputCol='features')

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
log_reg_titanic = LogisticRegression(featuresCol='features',labelCol='Survived')
pipeline = Pipeline(stages=[gender_indexer,embark_indexer,
                           gender_encoder,embark_encoder,
                           assembler,log_reg_titanic])
train_titanic_data, test_titanic_data = my_final_data.randomSplit([0.7,.3])
fit_model = pipeline.fit(train_titanic_data)
results = fit_model.transform(test_titanic_data)

from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='Survived')
results.select('Survived','prediction').show()
AUC = my_eval.evaluate(results)
AUC
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Consulting%20Project/Consulting%20Project%20-%20Linear%20Regression)

---

###### Decision Trees and Random Forests

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('rf').getOrCreate()
data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
data.show()

(trainingData, testData) = data.randomSplit([0.7, 0.3])
trainingData.printSchema()

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)
model = rf.fit(trainingData)
predictions = model.transform(testData)
predictions.printSchema()
predictions.select("prediction", "label", "features").show(5)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
model.featureImportances

gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
model = gbt.fit(trainingData)
predictions = model.transform(testData)
predictions.select("prediction", "label", "features").show(5)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

dtc = DecisionTreeClassifier()
model = dtc.fit(trainingData)
predictions = model.transform(testData)
predictions.select("prediction", "label", "features").show(5)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('treecode').getOrCreate()
data = spark.read.csv('College.csv',inferSchema=True,header=True)
data.printSchema()
data.head()

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=['Apps',
             'Accept',
             'Enroll',
             'Top10perc',
             'Top25perc',
             'F_Undergrad',
             'P_Undergrad',
             'Outstate',
             'Room_Board',
             'Books',
             'Personal',
             'PhD',
             'Terminal',
             'S_F_Ratio',
             'perc_alumni',
             'Expend',
             'Grad_Rate'],
    outputCol="features")
output = assembler.transform(data)

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="Private", outputCol="PrivateIndex")
output_fixed = indexer.fit(output).transform(output)
final_data = output_fixed.select("features",'PrivateIndex')
train_data,test_data = final_data.randomSplit([0.7,0.3])

from pyspark.ml.classification import DecisionTreeClassifier,GBTClassifier,RandomForestClassifier
from pyspark.ml import Pipeline

dtc = DecisionTreeClassifier(labelCol='PrivateIndex',featuresCol='features')
rfc = RandomForestClassifier(labelCol='PrivateIndex',featuresCol='features')
gbt = GBTClassifier(labelCol='PrivateIndex',featuresCol='features')

dtc_model = dtc.fit(train_data)
rfc_model = rfc.fit(train_data)
gbt_model = gbt.fit(train_data)

dtc_predictions = dtc_model.transform(test_data)
rfc_predictions = rfc_model.transform(test_data)
gbt_predictions = gbt_model.transform(test_data)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

acc_evaluator = MulticlassClassificationEvaluator(labelCol="PrivateIndex", predictionCol="prediction", metricName="accuracy")
dtc_acc = acc_evaluator.evaluate(dtc_predictions)
rfc_acc = acc_evaluator.evaluate(rfc_predictions)
gbt_acc = acc_evaluator.evaluate(gbt_predictions)

print('Decision Tree accuracy : {0:2.2f}%'.format(dtc_acc*100))
print('Random Forest Ensemble accuracy : {0:2.2f}%'.format(rfc_acc*100))
print('GBT accuracy : {0:2.2f}%'.format(gbt_acc*100))
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Consulting%20Project/Consulting%20Project%20-%20Random%20Forest)

---

###### K-Means Clustering

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('cluster').getOrCreate()

from pyspark.ml.clustering import KMeans
dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")

kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

wssse = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(wssse))

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
```

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('cluster').getOrCreate()

from pyspark.ml.clustering import KMeans
dataset = spark.read.csv("seeds_dataset.csv",header=True,inferSchema=True)

dataset.head()
dataset.describe().show()

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
dataset.columns
vec_assembler = VectorAssembler(inputCols = dataset.columns, outputCol='features')
final_data = vec_assembler.transform(dataset)

from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scalerModel = scaler.fit(final_data)
final_data = scalerModel.transform(final_data)

kmeans = KMeans(featuresCol='scaledFeatures',k=3)
model = kmeans.fit(final_data)
wssse = model.computeCost(final_data)
print("Within Set Sum of Squared Errors = " + str(wssse))
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
model.transform(final_data).select('prediction').show()
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Consulting%20Project/Consulting%20Project%20-%20K-Means%20Clustering)

---

#### Recommender System

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('rec').getOrCreate()

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

data = spark.read.csv('movielens_ratings.csv',inferSchema=True,header=True)
data.head()
data.describe().show()

(training, test) = data.randomSplit([0.8, 0.2])
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(training)
predictions = model.transform(test)
predictions.show()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

single_user = test.filter(test['userId']==11).select(['movieId','userId'])
single_user.show()
reccomendations = model.transform(single_user)
reccomendations.orderBy('prediction',ascending=False).show()
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Consulting%20Project/Consulting%20Project%20-%20Recommender%20System)

---

#### NLP

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('nlp').getOrCreate()

# Tokenizer
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

sentenceDataFrame = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,neat")
], ["id", "sentence"])
sentenceDataFrame.show()

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
countTokens = udf(lambda words: len(words), IntegerType())
tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words").withColumn("tokens", countTokens(col("words"))).show(truncate=False)
regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words").withColumn("tokens", countTokens(col("words"))).show(truncate=False)

# Stopwords
from pyspark.ml.feature import StopWordsRemover

sentenceData = spark.createDataFrame([
    (0, ["I", "saw", "the", "red", "balloon"]),
    (1, ["Mary", "had", "a", "little", "lamb"])
], ["id", "raw"])
remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
remover.transform(sentenceData).show(truncate=False)

# N-Grams
from pyspark.ml.feature import NGram

wordDataFrame = spark.createDataFrame([
    (0, ["Hi", "I", "heard", "about", "Spark"]),
    (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
    (2, ["Logistic", "regression", "models", "are", "neat"])
], ["id", "words"])
ngram = NGram(n=2, inputCol="words", outputCol="ngrams")
ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.select("ngrams").show(truncate=False)

# Feature Extractors
# 1. TF-IDF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])
sentenceData.show()

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
wordsData.show()

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
wordsData.show()

# 2. CountVectorizer
from pyspark.ml.feature import CountVectorizer

# Input data: Each row is a bag of words with a ID.
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])

# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)
model = cv.fit(df)
result = model.transform(df)
result.show(truncate=False)
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Spam%20Detection)

---

#### Spark Streaming

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext('local[2]', 'NetworkWordCount')
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream('localhost', 9999)
words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda word: (word ,1))
word_counts = pairs.reduceByKey(lambda num1, nums2: num1+num2)
word_counts.pprint()
ssc.start()
```

[Project](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/tree/main/Projects/Twitter%20Analytics)

---
