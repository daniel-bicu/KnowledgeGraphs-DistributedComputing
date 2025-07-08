// Q6: Which are the most similar papers with other paper? 

CALL gds.graph.project(
'paperSimilarityGraph',['Paper', 'Topic'],
{
ADDRESSES_TOPIC: {
type: 'ADDRESSES_TOPIC',
orientation: 'UNDIRECTED' }
})

query - using nodeSimilarity algorithm:
CALL gds.nodeSimilarity.stream('paperSimilarityGraph')
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS paper1, gds.util.asNode(node2) AS paper2,similarity
WHERE similarity > 0.3
WITH paper1, paper2, round(similarity, 3) AS sim
WITH paper1.eid as paperEID, paper1.title as paperTitle, collect({title: paper2.title, sim: sim})[0..3] AS topSimilar
RETURN paperEID, paperTitle, topSimilar
LIMIT 3