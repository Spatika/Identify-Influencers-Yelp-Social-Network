from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession.builder\
    .config("spark.jars.packages", "graphframes:graphframes:0.7.0-spark2.4-s_2.11")\
    .appName('Yelp_Influencers').getOrCreate()

from graphframes import *

# set new runtime options
spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.set("spark.executor.memory", "7g")
spark.conf.set("spark.driver.memory", "7g")


business = spark.read.json("yelp_dataset/business.json")

# The inferred schema can be visualized using the printSchema() method
business.printSchema()

business = business.withColumnRenamed("review_count", "review_num").withColumnRenamed("stars", "business_avg_rating")

toronto_business = business.select("business_id", "city", "review_num", "business_avg_rating", "name").filter(business.city == "Toronto")

users = spark.read.json("yelp_dataset/user.json")
users.printSchema()

users_lite = users.select('user_id', 'yelping_since', 'elite', 'friends', 'review_count').filter(users.friends != "None")
users_lite.printSchema()


reviews = spark.read.json("yelp_dataset/review.json")
reviews_lite = reviews.select("user_id", "business_id", "review_id", "stars", "date")


toronto_business_reviewers = toronto_business.join(reviews_lite, "business_id")
toronto_business_reviewers.printSchema()

toronto_users = toronto_business_reviewers.join(users_lite, "user_id")
toronto_users.write.csv("output1", mode='overwrite', header=True)


# exploded friends list - separated by comma + one  or more spaces
exploded = toronto_users.select("user_id", explode(split(toronto_users.friends, "(,\s*)")))
noDups = exploded.dropDuplicates()
noDups.write.csv("output2", mode='overwrite', header=True)

# list of unique user_ids in Toronto
vertices = toronto_users.selectExpr('user_id as id').distinct()
vertices.printSchema()

#filtered
unique_list = [item.user_id for item in toronto_users.select('user_id').distinct().collect()]
toronto_friends = noDups[noDups.col.isin(unique_list)]

# Create an Edge DataFrame with "src" and "dst" columns

edges = toronto_friends.selectExpr("user_id as src", "col as dst")
yelpGraph = GraphFrame(vertices, edges)

#### PART 1 & 2 - NO EDGE WEIGHTS, BIDIRECTIONAL (undirected)


#### PART 1 #####
# Query: Get in-degree of each vertex - neighborhood profiling
neighborhoodProfile = yelpGraph.inDegrees.sort("inDegree", ascending=False)
neighborhoodProfile.write.csv("profile", mode='overwrite', header=True)

################

#### PART 2 #####

# Run PageRank algorithm, and show results
results = yelpGraph.pageRank(resetProbability=0.01, maxIter=10)
resultsSorted = results.vertices.select("id", "pagerank").sort("pagerank", ascending=False)
resultsSorted.write.csv("pageRank", mode='overwrite', header=True)
################


#https://stackoverflow.com/questions/35570603/dealing-with-commas-within-a-field-in-a-csv-file-using-pyspark