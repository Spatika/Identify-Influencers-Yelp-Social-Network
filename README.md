# Identify Influencers in the Yelp Social Network

- PySpark on HDFS
- PageRank, Weighted PageRank

## Description 
- Project to identify influential users in a social network. Can be used for targeted marketing and to maximize diffusion of information. 
- A one star increase in a user review could translate to 5-9% more revenue - this could be more for a high review by a Yelp user with much reach and influence.

## Data
- Merge between Yelp User Attributes, Reviews + Business dataset - to get Toronto users, reviews & businesses
- Segmented to just one city - Toronto for a dense graph, and it makes sense to study their interactions/influence effects
- Filtered to get positive (>3 star) reviews, since we are interested in positive influence in this case
- Removed users who have written less than the average # of reviews
- Graph 1: One more filter to retain only users who have more than 500 friends
- Graph 2: All users, those who have more or less than 500 friends

## Graph 
- Each user is a node, connected by bi-directional edges with their Yelp friends 
- Nodes: 11,600 nodes and Edges: 172,588 edges v2: after filtering to those having 500 or more friends only: 708 nodes and 21.7K edges
- Edge Weight - influence of A on B is directed edge weight from A to B:
  - Depends on A’s: fan ratio, vote ratio, compliment ratio,
  - Depends on following ratio: # of reviews written by B after A (for the same restaurant) / total # of A’s reviews

## Method
- Used Weighted PageRank to rank and sort by top 100 users as ‘influencers’
- **Metric:** 
  -  Out of the top 100 influencers identified by our algorithm, how many of those has Yelp already tagged as ‘Elite’ status? 
  -  Alternatively, does our algorithm identify potentially influential users who have **not** been identified as Elite, that businesses on Yelp could target?
  -  Weighted PageRank successfully identified 3 and 5 "new Elite" users in Graphs 1 and 2 respectively
