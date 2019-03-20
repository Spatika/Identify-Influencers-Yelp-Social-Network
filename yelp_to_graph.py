from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import array_contains

import pandas as pd
# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession.builder\
    .config("spark.jars.packages", "graphframes:graphframes:0.7.0-spark2.4-s_2.11")\
    .appName('Yelp_Influencers').getOrCreate()

from graphframes import *

# set new runtime options
spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.set("spark.executor.memory", "7g")
spark.conf.set("spark.driver.memory", "7g")

#business = spark.read.csv('yelp_academic_dataset/yelp_business.csv', header=True)
# df = pd.read_csv('yelp_academic_dataset/yelp_business.csv')
#
# df = df[['business_id', 'name', 'city']].astype(str)
#
# business = spark.createDataFrame(df[['business_id', 'name', 'city']])

business = spark.read.json("yelp_dataset/business.json")

# The inferred schema can be visualized using the printSchema() method
business.printSchema()

business = business.withColumnRenamed("review_count", "review_num").withColumnRenamed("stars", "business_avg_rating")

toronto_business = business.select("business_id", "city", "review_num", "business_avg_rating", "name").filter(business.city == "Toronto")

# df = pd.read_csv('yelp_academic_dataset/yelp_user.csv')
# df = df[['user_id', 'friends']].astype(str)
users = spark.read.json("yelp_dataset/user.json")
users.printSchema()

cols = list(set(users.columns) - {'name', 'useful', 'funny', 'cool', 'compliment_writer', 'compliment_more',
                                  'compliment_profile', 'compliment_list',
                                  'compliment_hot', 'compliment_funny', 'compliment_cool', 'compliment_note'})

users_lite = users.select('user_id', 'yelping_since', 'elite', 'friends', 'review_count').filter(users.friends != "None")
users_lite.printSchema()
#
# df = pd.read_csv('yelp_academic_dataset/yelp_review.csv')
# df = df[['review_id', "user_id", 'business_id', 'stars', 'date', 'useful', 'funny', 'cool']].astype(str)

reviews = spark.read.json("yelp_dataset/review.json")
reviews_lite = reviews.select("user_id", "business_id", "review_id", "stars", "date")


toronto_business_reviewers = toronto_business.join(reviews_lite, "business_id")
toronto_business_reviewers.printSchema()
#toronto_business_reviewers.write.csv("output", mode='overwrite', header=True)

toronto_users = toronto_business_reviewers.join(users_lite, "user_id")
toronto_users.write.csv("output", mode='overwrite', header=True)

#filtered_users = toronto_users.select()

# list of unique user_ids in Toronto
vertices = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns

# list of unique pairs in Toronto
edges = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])


g = GraphFrame(vertices, edges)

#### PART 1 & 2 - NO EDGE WEIGHTS


#### PART 1 #####
# Query: Get in-degree of each vertex - neighborhood profiling
g.inDegrees.show()
################

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

#### PART 2 #####
# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
################


#https://stackoverflow.com/questions/35570603/dealing-with-commas-within-a-field-in-a-csv-file-using-pyspark