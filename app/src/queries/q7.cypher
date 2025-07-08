// Q7: Which are the most similar authors based on their papers similarities?

CALL gds.nodeSimilarity.stream('paperSimilarityGraph')
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS paper1, gds.util.asNode(node2) AS paper2, similarity
WHERE similarity > 0.8
WITH paper1, collect({paper: paper2, sim: round(similarity, 3)})[0..3] AS topSimPapers
LIMIT 5  

UNWIND topSimPapers AS simData
WITH paper1, simData.paper AS similarPaper, simData.sim AS simScore

MATCH (paper1)<-[:AUTHORED]-(a1:Author)
WITH paper1, similarPaper, simScore, collect(DISTINCT a1.author) AS authors1

MATCH (similarPaper)<-[:AUTHORED]-(a2:Author)
WITH 
  paper1.title AS paperTitle,
  similarPaper.title AS similarPaperTitle,
  authors1,
  collect(DISTINCT a2.author) AS authors2,
  simScore
RETURN 
  paperTitle, 
  similarPaperTitle, 
  authors1 AS paper1Authors, 
  authors2 AS similarPaperAuthors, 
  simScore
LIMIT 3