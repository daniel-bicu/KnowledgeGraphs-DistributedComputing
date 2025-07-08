# Unified Knowledge Discovery Framework

## 🎯 Project Aim

This project aims to bridge the gap between academic research and industry demands by building a **Unified Knowledge Discovery Framework**. It integrates heterogeneous datasets from academic databases (e.g., Scopus) and industry platforms (e.g., LinkedIn, Stack Overflow), models them using a **Knowledge Graph**, and applies **NLP**, **semantic classification**, and **graph analytics** to extract insights about the alignment and evolution of topics in Computer Science.

---

---

## ⚙️ Key Components

### 1. **Data Ingestion Jobs** (`app/src/jobs`)
- `scopus_etl.py`: Extracts research papers metadata.
- `linkedin_etl.py`, `stackoverflow_etl.py`: Ingest job roles and posts.
- `*_to_topics_etl.py`: Maps entities to CSO topics.

### 2. **CSO Integration**
- `cso_ingestor.py`: Parses and loads the Computer Science Ontology.
- `classify_papers.py`: NLP-based keyword classification using CSO.
- `embeddings_computing.py`: Computes sentence-transformer embeddings for topic alignment.

### 3. **Graph Construction**
- Entities (Papers, Authors, Jobs, Posts) are linked to `Topic` nodes using relationships such as:
  - `:ADDRESSES_TOPIC`
  - `:REQUIRES_KNOWLEDGE_OF`
  - `:IS_TAGGED_WITH`

### 4. **Queries** (`app/src/queries`)
- `q1.cypher` to `q8.cypher`: Predefined analytical queries, e.g.:
  - Topic trend evolution
  - Cross-domain topic relevance
  - Community detection (Louvain)
  - Knowledge gaps and alignment

---

## 📊 Graph Analytics

Graph algorithms from **Neo4j GDS** were used:
- **PageRank**: Identify influential topics.
- **Louvain / Leiden**: Community detection for topic clusters.
- **Betweenness Centrality**: Bridge topics across domains.

---

## 🧠 NLP & Semantic Strategies

- **CSO Classifier** for paper classification.
- **Topic embeddings** using `all-MiniLM-L6-v2` and cosine similarity.
- **Keyword normalization** and fuzzy matching.

---

## 🚀 Results Highlights

- Over **490K nodes** and **1.1M+ relationships** generated.
- Unified view across Academia, Industry, and Developer platforms.
- Identification of trending, aligned, and missing topics across domains.
- Topic-level evolution from **2012 to 2025**.

---

## 📌 Usage

1. Clone the repo:
   ```bash
   git clone <repo-url>

