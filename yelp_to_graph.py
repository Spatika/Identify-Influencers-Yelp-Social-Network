from pyspark.sql import SparkSession
# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession.builder\
    .config("spark.jars.packages", "graphframes:graphframes:0.7.0-spark2.4-s_2.11")\
    .appName('Yelp_Influencers').getOrCreate()

from graphframes import *

# set new runtime options
spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.set("spark.executor.memory", "6g")
spark.conf.set("spark.driver.memory", "6g")

business = spark.read.csv('yelp_academic_dataset/yelp_business.csv', header=True)

business_attributes = spark.read.csv('yelp_academic_dataset/yelp_business_attributes.csv', header=True)

vertices = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
edges = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])


g = GraphFrame(vertices, edges)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()