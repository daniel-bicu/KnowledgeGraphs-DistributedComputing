from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import os
import re

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

# Reading DFs
companies_df = spark.read.csv('../../data/bronze_layer/linkedin/2023-2024/companies.csv', inferSchema=True, header=True)

industries_df = spark.read.csv('../../data/bronze_layer/linkedin/2023-2024/industries.csv', inferSchema=True, header=True)
job_industries_df = spark.read.csv('../../data/bronze_layer/linkedin/2023-2024/job_industries.csv', inferSchema=True, header=True)

jobs_df = spark.read.csv('../../data/bronze_layer/linkedin/2023-2024/postings.csv', inferSchema=True, header=True)

print('[TRANSFORMATIONS]...')
# create denormalized view for enrichment data
denormalized_jobs_df = jobs_df.join(job_industries_df, 'job_id')\
                              .join(industries_df, 'industry_id')

# drop unimportant cols
denormalized_jobs_df = denormalized_jobs_df.drop('salary_id', 'industry_id')

# filtering domains
regex_expr = r'(?i)\b(IT|SOFTWARE|TECHNOLOGY|INTELLIGENCE|RESEARCH)\b'
denormalized_jobs_df_IT_related = denormalized_jobs_df\
    .filter(col('industry_name').rlike(regex_expr))\
    .withColumn('company_id', col("company_id").cast("long"))


print('[INGESTING] Writting nodes >> :Job')
# denormalized_jobs_df_IT_related.write \
#                             .format("org.neo4j.spark.DataSource")\
#                             .mode("Overwrite")\
#                             .option("labels", ":Job")\
#                             .option("node.keys", "job_id")\
#                             .save()


print('[TRANSFORMATIONS] Splitting into multiple entities...')

def normalize_title(title: str):
    if not title:
        return ""
    
    title = title.lower().strip()

    # List of levels to remove
    levels = ['junior', 'jr', 'mid', 'middle', 'mid-level', 'senior', 'sr', 'lead', 'principal', 'staff']

    # Remove level keywords
    pattern = r'\b(' + '|'.join(levels) + r')\b'
    title = re.sub(pattern, '', title)

    # Clean up extra spaces
    title = re.sub(r'\s+', ' ', title).strip()

    # Capitalize first letter of each word if needed
    return title

normalize_title = udf(normalize_title, StringType())


jobs_role_df = denormalized_jobs_df_IT_related\
    .select('job_id', 'title', 'description')\
    .distinct()\
    .dropna(subset=['title'])\
    .withColumn('name', normalize_title(col('title')))
  

print('[INGESTING] Writting nodes >> :JobRole')
# jobs_role_df.drop('job_id', 'title', 'description').write \
#                             .format("org.neo4j.spark.DataSource")\
#                             .mode("Overwrite")\
#                             .option("labels", ":JobRole")\
#                             .option("node.keys", "name")\
#                             .save()

print('[INGESTING] Writting relationship >> :Job -[:HAS_ROLE]-> :JobRole')
# jobs_role_df.write \
#     .format("org.neo4j.spark.DataSource") \
#     .mode("overwrite") \
#     .option("relationship", "HAS_ROLE") \
#     .option("relationship.save.strategy", "keys") \
#     .option("relationship.source.labels", ":Job") \
#     .option("relationship.source.node.keys", "job_id:job_id") \
#     .option("relationship.target.labels", ":JobRole") \
#     .option("relationship.target.node.keys", "name:name") \
#     .save()


companies_df_IT_related = companies_df\
    .withColumn('company_id', col("company_id").cast("long"))\
    .filter(col("company_id").isNotNull())\
    .join(denormalized_jobs_df_IT_related\
          .filter(col("company_id").isNotNull()),\
         "company_id",\
         how = 'leftsemi')

# print('Nr of IT companies: ', companies_df.count())

print('[INGESTING] Writting nodes >> :Company')
# companies_df_IT_related.write\
#     .format("org.neo4j.spark.DataSource")\
#     .mode("Overwrite")\
#     .option("labels", ":Company")\
#     .option("node.keys", "company_id")\
#     .save()

print('[INGESTING] Writting relationship >> :Company -[:POST]-> :Job')
# denormalized_jobs_df_IT_related.write \
#     .format("org.neo4j.spark.DataSource") \
#     .mode("overwrite") \
#     .option("relationship", "POST") \
#     .option("relationship.save.strategy", "keys") \
#     .option("relationship.source.labels", ":Company") \
#     .option("relationship.source.node.keys", "company_id:company_id") \
#     .option("relationship.target.labels", ":Job") \
#     .option("relationship.target.node.keys", "job_id:job_id") \
#     .save()
