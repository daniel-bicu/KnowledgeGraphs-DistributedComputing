// Q8: How should I identify which will be the potential hubs of authors (Scopus), job roles(LinkedIn), and communities (StackOverflow Posts)?

CALL gds.graph.project('fullKnowledgeGraph',['Topic', 'Paper', 'JobRole', 'StackOverflowPost', 'Author', 'Company'],
{
ADDRESSES_TOPIC: { type: 'ADDRESSES_TOPIC', orientation: 'UNDIRECTED' },
REQUIRES_KNOWLEDGE_OF: { type: 'REQUIRES_KNOWLEDGE_OF', orientation: 'UNDIRECTED' },
IS_TAGGED_WITH: { type: 'IS_TAGGED_WITH', orientation: 'UNDIRECTED' },
AUTHORED: { type: 'AUTHORED', orientation: 'UNDIRECTED' },
POST: { type: 'POST', orientation: 'UNDIRECTED' },
HAS_ROLE: { type: 'HAS_ROLE', orientation: 'UNDIRECTED' }})
