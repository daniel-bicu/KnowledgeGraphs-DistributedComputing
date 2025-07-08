from sentence_transformers import SentenceTransformer
import pandas as pd
import json
import re

def remove_topics_based_on_keywords(items, keywords):
    return [item for item in items if not re.search(f".*{'|'.join(keywords)}.*", item)]


def extract_topics_v3(d, key, depth=1, max_depth=3, seen=None):
    if seen is None:
        seen = set()
        
    topics = []
    key_topics = d.get(key, [])
    
    for subkey in key_topics:
        if subkey not in seen:
            seen.add(subkey)
            topics.append(subkey)
            if depth < max_depth:
                topics.extend(extract_topics_v3(d, subkey, depth + 1, max_depth, seen))
    
    return topics


print("Loading CSO hierarchy...")
with open('../../data/silver_layer/cso/cso_hierarchy_canonical.json', 'r') as f:
    cso = json.load(f)

print('Extracting topics...')

top_topics = extract_topics_v3(d=cso, key='computer_science', depth=1,  max_depth=3)
print('before', len(top_topics))
stop_keywords = ["computer_science", "internet", "computer", "software", "engineering", "engineers", "information", "technology", "teaching", "system"]

 # remove too general topics
top_topics = remove_topics_based_on_keywords(top_topics, stop_keywords)

print('after', len(top_topics))

print('Loading all-MiniLM-L6-v2 model...')
model = SentenceTransformer("all-MiniLM-L6-v2")

df_jobs = pd.read_csv("../../data/silver_layer/linkedin/linkedin_jobs.csv", header=None, names=["job_title"],
                      sep=';', quotechar='"', on_bad_lines='warn')

print('Computing word embeddings for linkedin jobs...')
# job_titles = df_jobs["job_title"].fillna("").tolist()
# job_embeddings = model.encode(job_titles, batch_size=64, show_progress_bar=True, convert_to_numpy=True)
# df_jobs["embedding"] = list(job_embeddings)

print('Computing word embeddings for CSO Topics...')
# topic_embeddings = model.encode(top_topics, batch_size=32, show_progress_bar=True, convert_to_numpy=True)
# df_topics = pd.DataFrame({"topic": top_topics, "embedding": list(topic_embeddings)})


print('Computing word embeddings for stackoverflow posts...')
df_stackoverflow_posts = pd.read_csv("../../data/silver_layer/stackoverflow/questions/stackoverflow_posts.csv", encoding="utf-8", quotechar='"', on_bad_lines='warn')

stackoverflow_posts_text = df_stackoverflow_posts["body"].fillna("").str.cat(df_stackoverflow_posts["tags"].fillna(""), sep="\n").tolist()
stackoverflow_posts_embeddings = model.encode(stackoverflow_posts_text, batch_size=64, show_progress_bar=True, convert_to_numpy=True)
df_stackoverflow_posts["embedding"] = list(stackoverflow_posts_embeddings)


# print('Storing Linkeding job embeddings...')
# df_jobs.to_parquet("../../data/silver_layer/embeddings/job_embeddings.parquet", index=False)

# print('Storing CSO topics embeddings...')
# df_topics.to_parquet("../../data/silver_layer/embeddings/cso_embeddings.parquet", index=False)

print('Storing Stackoverflow posts embeddings...')
df_stackoverflow_posts.to_parquet("../../data/silver_layer/embeddings/stackoverflow_posts_embeddings.parquet", index=False)