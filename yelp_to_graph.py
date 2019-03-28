from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import mean as _mean, col, length, size


# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession.builder\
    .config("spark.jars.packages", "graphframes:graphframes:0.7.0-spark2.4-s_2.11")\
    .appName('Yelp_Influencers').getOrCreate()

from graphframes import *

# set new runtime options
spark.conf.set("spark.sql.shuffle.partitions", 20)
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("maximizeResourceAllocation", "true")
spark.conf.set("spark.executor.memoryOverhead", "3g")

business = spark.read.json("yelp_dataset/business.json")

# The inferred schema can be visualized using the printSchema() method
business.printSchema()

# old, new
business = business.withColumnRenamed("review_count", "review_num").withColumnRenamed("stars", "business_avg_rating")

toronto_business = business.select("business_id", "city", "review_num", "business_avg_rating", "name").filter(business.city == "Toronto")

users = spark.read.json("yelp_dataset/user.json")
users.printSchema()

#  filter users with review_count > average number of reviews
users_with_friends = users.select('user_id', 'yelping_since', 'elite', 'friends', 'review_count')\
    .filter(users.friends != "None")

df_stats = users_with_friends.select(
    _mean(col("review_count")).alias('mean')).collect()

mean = df_stats[0]['mean']
users_lite = users_with_friends.filter(users_with_friends.review_count > mean)


# keep only positive reviews = filter stars > 3
reviews = spark.read.json("yelp_dataset/review.json")
reviews_lite = reviews.select("user_id", "business_id", "review_id", "stars", "date")\
    .filter((reviews.stars == "4.0") | (reviews.stars == "5.0"))


toronto_business_reviewers = toronto_business.join(reviews_lite, "business_id")
toronto_business_reviewers.printSchema()

toronto_users = toronto_business_reviewers.join(users_lite, "user_id")
toronto_users.printSchema()

# keep users only with # of friends > 500
toronto_users_friend_list = toronto_users.withColumn('friends_list', split(toronto_users.friends, "(,\s*)"))
toronto_users_min_friends = toronto_users_friend_list.filter(size(toronto_users_friend_list.friends_list) > 100)

#toronto_users_min_friends.select("user_id", "num_friends").sort("num_friends").write.csv("output", mode='overwrite', header=True)

# exploded friends list - separated by comma + one  or more spaces
exploded = toronto_users_min_friends.select("user_id", explode(split(toronto_users.friends, "(,\s*)")))
noDups = exploded.dropDuplicates()


# list of unique user_ids in Toronto
vertices = toronto_users_min_friends.selectExpr('user_id as id', 'elite').distinct()
vertices.printSchema()

#filtered
unique_list = [item.user_id for item in toronto_users_min_friends.select('user_id').distinct().collect()]
toronto_friends = noDups[noDups.col.isin(unique_list)]

# Create an Edge DataFrame with "src" and "dst" columns

edges = toronto_friends.selectExpr("user_id as src", "col as dst")
yelpGraph = GraphFrame(vertices, edges)

#### PART 1 & 2 - NO EDGE WEIGHTS, BIDIRECTIONAL (undirected)


#### PART 1 #####
# Query: Get in-degree of each vertex - neighborhood profiling - 1-hop
oneHop = yelpGraph.inDegrees

# need to print this with 'Elite' attribute too - join with original vertices list
neighborhoodProfile = oneHop.join(vertices,  "id").sort("inDegree", ascending=False)
neighborhoodProfile.write.csv("profile", mode='overwrite', header=True)

# 2-hop neighbours
pattern = "(x1) - [a] -> (x2); (x2) - [b] -> (x3); !(x1)-[]->(x3)" # 2nd hop cannot be a vertex already reachable in 1 hop
paths = yelpGraph.find(pattern).filter("(x3!=x1)") # 2nd hop cannot be itself

groupedBySource = paths.groupBy("x1").count()
neighborhoodprofile2 = groupedBySource.select("x1.id", "x1.elite", "count").sort("count", ascending=False)
neighborhoodprofile2.printSchema()
neighborhoodprofile2.write.csv("profile2", mode='overwrite', header=True)


################

#### PART 2 #####

# Run PageRank algorithm, and show results
results = yelpGraph.pageRank(resetProbability=0.01, maxIter=10)
resultsSorted = results.vertices.select("id", "pagerank", "elite").sort("pagerank", ascending=False)
resultsSorted.write.csv("pageRank", mode='overwrite', header=True)
################


#https://stackoverflow.com/questions/35570603/dealing-with-commas-within-a-field-in-a-csv-file-using-pyspark