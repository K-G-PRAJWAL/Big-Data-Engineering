 # [Apache Spark with Python - Big Data with PySpark and Spark](https://www.packtpub.com/product/apache-spark-with-python-big-data-with-pyspark-and-spark-video/9781789133394)

 ### Spark

Apache Spark is a fast, in-memory data processing engine which allows data workers to efficiently execute streaming, machine learning or SQL workloads that require fast iterative access to datasets.

---

### RDD

An RDD is simply a capsulation around a very large dataset. In Spark all work is expressed as either creating new RDDs, transforming existing RDDs, or calling operations on RDDs to compute a result.

---

### Operations on RDD

- Transformations = Apply some functions to the data in RDD to create a new RDD.
- Actions = Compute a result based on an RDD.

---

### Create RDD

```python
inputNums = list(range(1, 5))
RDD = sc.parallelize(inputNums)
RDD = sc.textFile('folder/file.txt')
```

---

### Transformations

- filter()        = Takes in a function and returns an RDD formed by selecting those elements which pass the filter function.
- map()           = Takes in a function and passes each element in the input RDD through the function, with the result of the  function being the new value of each element in the resulting RDD.
- flatMap()       = It takes each element from an existing RDD and it can produce 0, 1 or many outputs for each element.
- Set Operations  = sample, distinct, union, intersection, subtract, cartesian product

---

### Actions

- collect()       = Retrieves the entire RDD and returns it to the driver program in the form of a regular collection or value
- count()         = A quick way to count the number of rows in an RDD.
- countByValue()  = Look at unique values in each row of RDD and return a map of each unique value to its value.
- take()          = Take a peek into the RDD for unit tests and debugging.
- saveAsTextFile()= Write data out to a distributed storage system such as HDFS or S3.
- reduce()        = Takes a function that operates on 2 elements of the type in input RDD and returns a new element of same   type.

---

### Properties of RDD

- RDD's are Distributed
- RDD's are Immutable
- RDD's are Resilient
- RDD's are Lazily Evaluated

---

### Persistence

Reuse a RDD in multiple actions by calling persist() method on RDD. The first time it is computed, it will be kept in memory across the nodes.

Storage Levels:
- MEMORY_ONLY(RDD.cache())
- MEMORY_AND_DISK
- MEMORY_ONLY_SER
- MEMORY_AND_DISK_SER
- DISK_ONLY

---

### Spark SQL

Spark package for working with structured data which is built on top of spark core. Provides a SQL like interface for working with structured data.

---

### Spark Streaming

Provides an API for manipulating data streams that closely match the Spark core's RDD API.

---

### MLlib

It is a scalable machine leranig library that delivers both high-quality algorithms and blazing speed.

---

### GraphX

A graph computation engine built on top of Spark that enables users to interactively create, transform and reason about graph structured data scale.

---

### Pair RDD

Data is usually of key-value pair type. A pair RDD is a particular type of RDD that can store key-value pairs.

---

### Advanced

- Accumulators: variables that are used for aggregating information across the executors.
- Broadcast Variables: keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.

---

### Spark SQL

provides a data abstraction that simplifies working with structured dataset.

---

### DataFrame

- A data abstraction or domain-specific language for working with structured and semi-structured data.
- Store data in a more efficient manner copared to RDD's, taking advantage of their schema.
- It uses immutable, in-memory, resilient, distributed and parallel capabilities of RDD and applies a structure called schema to data. This allows spark to manage the schema and only pass data between nodes, in a much more efficient way than using Java serialization.
- Untyped view of dataset

---

### Dataset

- Object oriented programming style
- Compile time safety of RDD API
- leveraging schema to work with structured data
- Takes 2 distinct API characteristics: a strongly-typed API and an untyped API

---

### Catalyst Optimizer

SparkSQL uses an optimizer called Catalyst to optimize all the queries written both in Spark SQL and Dataframe DSL. Queries run much faster than their RDD counterparts.
Catalyst is a rule based library which is built as a rule-based system. Each rule in the framework focuses on the specific optimization.

---

### Caching

Spark SQL uses an im-memory columnar storage for the dataframe. Minimizes data read, automatically tune compression and reduce garbage collection and memory usage.

---

### Amazon EMR

Amazon Elastic MapReduce is a managed Hadoop framework that makes it easy, fast and cost-effective to process vast amounts of data across dynamically scalable Amazon EC2 instances.
