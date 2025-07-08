from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv
import os

load_dotenv()
neo4j_url = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
neo4j_db = os.getenv("NEO4J_PASSWORD")


# Config spark
def init_spark(config=None):
    spark = SparkSession.builder\
        .appName(name='LinkedinJobProcessor')\
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.6_for_spark_3") \
        .config("neo4j.url", neo4j_url) \
        .config("neo4j.authentication.type", "basic") \
        .config("neo4j.authentication.basic.username", neo4j_user) \
        .config("neo4j.authentication.basic.password", neo4j_pwd) \
        .config("neo4j.authentication.basic.password", neo4j_db) \
        .getOrCreate()
    
    return spark

# main
spark = init_spark()

embeddings_job_df = spark.read.parquet("../../data/silver_layer/embeddings/job_embeddings.parquet").repartition(8)
embeddings_topic_df = spark.read.parquet("../../data/silver_layer/embeddings/cso_embeddings.parquet").repartition(8)

for i in range(384):  # dimensiune MiniLM
    embeddings_job_df = embeddings_job_df.withColumn(f"job_{i}", col("embedding")[i])
    embeddings_topic_df = embeddings_topic_df.withColumn(f"topic_{i}", col("embedding")[i])


joined = embeddings_job_df.crossJoin(embeddings_topic_df)

# Similaritate cosine: (A·B) / (||A|| * ||B||)
dot_expr = " + ".join([f"job_{i} * topic_{i}" for i in range(384)])
norm_a = " + ".join([f"job_{i} * job_{i}" for i in range(384)])
norm_b = " + ".join([f"topic_{i} * topic_{i}" for i in range(384)])

similarity_expr = f"({dot_expr}) / (sqrt({norm_a}) * sqrt({norm_b}))"

joined = joined.withColumn("cosine_similarity", expr(similarity_expr).cast(DoubleType()))
joined = joined.filter(col("cosine_similarity") >= 0.4)

w = Window.partitionBy("job_title").orderBy(col("cosine_similarity").desc())

top_matches = joined.withColumn("rank", row_number().over(w)).filter("rank <= 3").select("job_title", "topic", "cosine_similarity")

# top_matches.show(n=50, truncate=False)

print('[INGESTING] Writting relationship >> :JobRole -[:REQUIRES_KNOWLEDGE_OF]-> :Topic')
top_matches.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("overwrite") \
    .option("relationship", "REQUIRES_KNOWLEDGE_OF") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":JobRole") \
    .option("relationship.source.node.keys", "job_title:name") \
    .option("relationship.target.labels", ":Topic") \
    .option("relationship.target.node.keys", "topic:name") \
    .option("relationship.properties", "cosine_similarity") \
    .save()
