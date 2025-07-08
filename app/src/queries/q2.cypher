// Q2: Which Topics are addressed only in one domain and are absent in others?
MATCH (t: Topic)
OPTIONAL MATCH (p:Paper)-[:ADDRESSES_TOPIC]->(t)
WITH t, count(p) AS paperCount
OPTIONAL MATCH (j:JobRole)-[:REQUIRES_KNOWLEDGE_OF]->(t)
WITH t, paperCount, count(j) AS jobCount
OPTIONAL MATCH (s:StackOverflowPost)-[:IS_TAGGED_WITH]->(t)
WITH t.name AS topic, paperCount, jobCount, count(s) AS soCount
WHERE soCount = 0 AND jobCount = 0 AND paperCount > 0
RETURN topic
ORDER BY paperCount DESC
LIMIT 10