// Q1: Which topics are used the most across all three sources: Scopus, LinkedIn, and Stackoverflow?

CALL {MATCH (p:Paper)
RETURN count(p) AS totalPapers}
CALL {MATCH (j:JobRole)
RETURN count(j) AS totalJobs}
CALL {MATCH (s:StackOverflowPost)
RETURN count(s) AS totalPosts}
WITH totalPapers, totalJobs, totalPosts
MATCH (t:Topic)
OPTIONAL MATCH (p:Paper)-[:ADDRESSES_TOPIC]->(t)
WITH t, count(p) AS paperCount, totalPapers, totalJobs, totalPosts
OPTIONAL MATCH (j:JobRole)-[:REQUIRES_KNOWLEDGE_OF]->(t)
WITH t, paperCount, count(j) AS jobCount, totalPapers, totalJobs, totalPosts
OPTIONAL MATCH (s:StackOverflowPost)-[:IS_TAGGED_WITH]->(t)
WITH t.name AS topic, paperCount, jobCount, count(s) AS soCount, totalPapers, totalJobs, totalPosts
WITH topic, paperCount, jobCount, soCount, round(100.0 * paperCount / totalPapers, 2) AS paperPct, round(100.0 * jobCount / totalJobs, 2) AS jobPct, round(100.0 * soCount / totalPosts, 2) AS soPct, round((paperCount + jobCount + soCount) / 3.0, 2) AS rawAvgCount, round(( (100.0 * paperCount / totalPapers) + (100.0 * jobCount / totalJobs) + (100.0 * soCount / totalPosts) ) / 3.0, 2) AS avgPct
RETURN topic, paperCount, jobCount, soCount, paperPct, jobPct, soPct,avgPct
ORDER BY avgPct DESC
LIMIT 10