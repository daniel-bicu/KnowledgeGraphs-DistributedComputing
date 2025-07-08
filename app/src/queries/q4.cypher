// Q4: What is the trend of topics that are popular in academic research over the years?
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
RETURN 
  topic, 
  total, 
  yearlyCounts, 
  countStart, 
  countEnd, 
  (countEnd - countStart) AS growth
ORDER BY growth DESC
LIMIT 10
