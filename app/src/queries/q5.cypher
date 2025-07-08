Q5: Having this analysis of papers' topics that have grown over the years and the topics resulted - how they match with the other domains’ data: LinkedIn Jobs and StackOverflow posts?

CALL {
  MATCH (p:Paper)-[:ADDRESSES_TOPIC]->(t:Topic)-[:SUBTOPIC_OF]->(:Topic)
  WHERE p.year IN [2012, 2023, 2024, 2025]
  WITH t.name AS topic, p.year AS year, count(*) AS count
  ORDER BY year
  WITH topic, collect({year: year, count: count}) AS yearlyCountsUnsorted, sum(count) AS total
  UNWIND yearlyCountsUnsorted AS yc
  WITH topic, total, yc
  ORDER BY topic, yc.year
  WITH topic, total, collect(yc) AS yearlyCounts,
       reduce(cs = 0, y IN [x IN collect(yc) WHERE x.year = 2012] | cs + y.count) AS countStart,
       reduce(ce = 0, y IN [x IN collect(yc) WHERE x.year = 2025] | ce + y.count) AS countEnd
  RETURN topic, (countEnd - countStart) AS growth
  ORDER BY growth DESC
  LIMIT 10
} 
WITH collect(topic) AS topTopics
UNWIND topTopics AS topicName
MATCH (t:Topic)
WHERE t.name = topicName
OPTIONAL MATCH (j:JobRole)-[:REQUIRES_KNOWLEDGE_OF]->(t)
WITH t, topicName, collect(DISTINCT j) AS jobRoles
OPTIONAL MATCH (s:StackOverflowPost)-[:IS_TAGGED_WITH]->(t)
WITH topicName AS topic, size(jobRoles) AS jobCount, count(DISTINCT s) AS soCount
WHERE jobCount > 0 AND soCount > 0
RETURN topic, jobCount, soCount
ORDER BY jobCount DESC
