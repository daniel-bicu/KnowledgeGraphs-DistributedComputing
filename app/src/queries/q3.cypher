// Q3: Which Topics are present in all the domains?
MATCH (t:Topic)
OPTIONAL MATCH (t)<-[:ADDRESSES_TOPIC]-(p:Paper)
OPTIONAL MATCH (t)<-[:IS_TAGGED_WITH]-(s:StackOverflowPost)
OPTIONAL MATCH (t)<-[:REQUIRES_KNOWLEDGE_OF]-(j:JobRole)
WITH 
  t.name AS topic, 
  count(DISTINCT p) > 0 AS inPapers,
  count(DISTINCT s) > 0 AS inStackOverflow,
  count(DISTINCT j) > 0 AS inJobs
WHERE inPapers AND inStackOverflow AND inJobs
RETURN topic
LIMIT 5