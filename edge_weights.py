
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from graphframes import *

# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession.builder.config("spark.master","local").appName('Yelp_Influencers').getOrCreate()

# set new runtime options
spark.conf.set("spark.sql.shuffle.partitions", 20)
spark.conf.set("spark.executor.memory", "7g")
spark.conf.set("spark.driver.memory", "7g")


## Read all the required data
#Read all the required input jason files
business_df = spark.read.json("C:/Personal/MSBA/Semester_2/Big Data/Final_project/Dataset/business.json")
reviews_df = spark.read.json("C:/Personal/MSBA/Semester_2/Big Data/Final_project/Dataset/review.json")
users_df = spark.read.json("C:/Personal/MSBA/Semester_2/Big Data/Final_project/Dataset/user.json")

#Convert date to yyyy/mm/dd format
from pyspark.sql.functions import expr

reviews_df = reviews_df.withColumn("date",expr("to_date(date)"))

#Create tables to run SQL queries
users_df.registerTempTable("userDB")
business_df.registerTempTable("businessDB")
reviews_df.registerTempTable("reviewsDB")


## Preprocessing steps

## Step 1: Remove users having no friends and users having less than average number of reviews

#Remove users who have no friends and less than average number of reviews
users = spark.sql("Select user_id,review_count,friends,elite,fans,useful+funny+cool as total_votes,average_stars,compliment_hot,compliment_morecompliment_profilecompliment_cutecompliment_listcompliment_notecompliment_plain from userDB where friends!=\"None\" and review_count > (Select AVG(review_count) from userDB)" )
users.show()

## Step 2: Remove reviews having a star rating of less than 3

#We are considering only positive reviews -> influencers needs to be identified for positive market growth
positive_review = spark.sql("Select review_id,user_id,business_id,stars,date from reviewsDB where stars>3")
positive_review.show()
print("The number of reviews in our dataset is:",positive_review.count())


## Step3: Subset for the business id present in the city of Toronto

#We are subsetting for a particular city ->Toranto
business_toranto = spark.sql("Select business_id from businessDB where city = \"Toronto\" ")
business_toranto.show()
print("The number of business ids present in Toronto:",business_toranto.count()

#Create temp tables for joining the dataframes
business_toranto.registerTempTable("torantobusinessDB")
positive_review.registerTempTable("positivereviewsDB")
users.registerTempTable("filteredUsersDB")


## Step4: Join the tables to contain reviews written only for the city of Toronto

#Join businessids and reviews to get users who have written reviews only for resturarents in Toranto
users_toranto = spark.sql("Select user_id,date,positivereviewsDB.business_id from positivereviewsDB,torantobusinessDB where torantobusinessDB.business_id = positivereviewsDB.business_id")
users_toranto.registerTempTable("usersTorantoDB")

#Select only distict users to join with the user attributes table
distinct_toranto_users = spark.sql("Select distinct(user_id) from usersTorantoDB ")
distinct_toranto_users.registerTempTable("distinct_usersTorantoDB")

## Step5: Join with the users table to obtain only the users who have written reviews for a resturarent in Toronto

#Join the users with the user table to get the friends of the user
influencer_users = spark.sql("Select filteredUsersDB.user_id,friends,elite from filteredUsersDB,distinct_usersTorantoDB where filteredUsersDB.user_id = distinct_usersTorantoDB.user_id")
influencer_users.show()
print("The total number of users who have written reviews for a resturarent in Toronto:",influencer_users.count()
#%% md
## Step6: Remove users having less than 500 friends
#%%
#Remove users whose total friends count is lesser than the average number of friends
filtered_influencer = influencer_users.select(influencer_users.user_id,explode(split(influencer_users.friends, "(,\s*)"))).groupBy('user_id').count()
filtered_influencer = filtered_influencer.withColumnRenamed('count','total_friends_count')


#Remove users having friends count lower than average ->redo code for now fixed at 500
filtered_influencer = filtered_influencer.select(filtered_influencer.user_id,filtered_influencer.total_friends_count).where(filtered_influencer.total_friends_count > 500)
print("The total number of users after filtering:",filtered_influencer.count()


## Creation of the graph
filtered_influencer = filtered_influencer.join(influencer_users,["user_id"])

## Step1: Vertices of the graph

#Create the vertices of the graph -> Users
vertices = filtered_influencer.select(filtered_influencer.user_id)
print("The total number of vertices in our graph:",vertices.show())



## Step2: Edges of the graph

#Create edges of the graph
edges =  filtered_influencer.select(filtered_influencer.user_id.alias("Source"),explode(split(filtered_influencer.friends, "(,\s*)")).alias("Dest"))


#Remove Dest nodes which are not in the Source
edges.registerTempTable("edgesDB")

#Takes very long time to run
edges_filtered = spark.sql("Select Source,Dest from edgesDB where Dest in (Select Source from edgesDB)")


## Step3: Calculating the edge weights

#Count the number of reviews written by the friends after the influencer has written a review for the same restuarent
edges_filtered.registerTempTable("edgesfilteredDB")

temp_table =  spark.sql("Select S2.business_id,S2.date,S2.user_id from edgesfilteredDB as S1,usersTorantoDB as S2 where S1.Source = S2.user_id")
temp_table.show(3)
temp_table.registerTempTable("CorrelatedTableDB")


#Try for only one edge

query = "Select E.Source,E.Dest,count(*) from edgesfilteredDB as E,CorrelatedTableDB as C,usersTorantoDB as U where E.Source = C.user_id and C.business_id = U.business_id and U.user_id = E.Dest and CAST(C.date as date) < CAST(U.date as date) group by E.Source,E.Dest"
reviews_after_influencer = spark.sql(query)

reviews_after_influencer.show(5)
