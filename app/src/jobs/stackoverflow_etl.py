from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, pandas_udf, regexp_replace
from pyspark.sql.types import StructField, StructType, LongType, StringType, IntegerType, TimestampType
from dotenv import load_dotenv
import os
import re
import html2text
import pandas as pd

load_dotenv()
neo4j_url = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
neo4j_db = os.getenv("NEO4J_PASSWORD")

def init_spark(config=None):
    spark = SparkSession.builder\
        .appName(name='StackOverflowJobProcessor')\
        .config("spark.sql.shuffle.partitions", '10') \
        .master("local[*]")\
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.6_for_spark_3," "com.databricks:spark-xml_2.12:0.15.0") \
        .config("neo4j.url", neo4j_url) \
        .config("neo4j.authentication.type", "basic") \
        .config("neo4j.authentication.basic.username", neo4j_user) \
        .config("neo4j.authentication.basic.password", neo4j_pwd) \
        .config("neo4j.authentication.basic.password", neo4j_db) \
        .getOrCreate()
    
    return spark

posts_schema = StructType([
    StructField("_Id", LongType(), True),
    StructField("_PostTypeId", IntegerType(), True),
    StructField("_CreationDate", TimestampType(), True),
    StructField("_Score", IntegerType(), True),
    StructField("_ViewCount", IntegerType(), True),
    StructField("_Body", StringType(), True),
    StructField("_Title", StringType(), True),
    StructField("_Tags", StringType(), True),
    StructField("_AnswerCount", IntegerType(), True),
    StructField("_CommentCount", IntegerType(), True),
    StructField("_FavoriteCount", IntegerType(), True)
])

def to_snake_case(name):
    name = name.lstrip("_")
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

converter = html2text.HTML2Text()
converter.ignore_links = True
converter.ignore_images = True
converter.ignore_emphasis = True
converter.body_width = 0

@pandas_udf(StringType())
def clean_body_udf(body_series: pd.Series) -> pd.Series:
    def clean(text):
        if pd.isna(text):
            return ""
        text = converter.handle(text)
        text = re.sub(r'https?://\S+', '', text)                   # remove URLs
        text = re.sub(r'[{}\[\]<>\"\'\*\_`]', '', text)            # remove strange chars
        text = re.sub(r'\n+', ' ', text)                           # remove newlines
        text = re.sub(r'\s+', ' ', text).strip()                   # normalize whitespace
        return text.lower()                                        # optional: lowercase
    return body_series.apply(clean)

if __name__ == "__main__":
    spark = init_spark()

    stackoverflow_df = spark.read\
    .format("xml") \
    .option("rowTag", "row") \
    .schema(posts_schema)\
    .load("../../data/bronze_layer/stackoverflow/2010_2025/Posts.xml")

    target_years = [2012, 2022, 2023, 2024, 2025]

    stackoverflow_questions_df = stackoverflow_df\
        .filter(year(col("_CreationDate")).isin(target_years)) \
        .filter(col("_PostTypeId") != 2) # filtering out answers posts

    stackoverflow_questions_processed = stackoverflow_questions_df\
        .withColumn('body', clean_body_udf(col('_Body'))).drop('_Body')\
        .withColumn("tags",regexp_replace(regexp_replace("_Tags", r"^<|>$", ""), r"><", ",")).drop('_Tags')

    stackoverflow_questions_processed = stackoverflow_questions_processed.toDF(*[to_snake_case(col) for col in stackoverflow_questions_processed.columns])

    stackoverflow_questions_processed\
        .coalesce(1)\
        .write\
        .option("header", True)\
        .option("quoteAll", True)\
        .mode("overwrite") \
        .csv('../../data/silver_layer/stackoverflow/questions')
    
    print('[INGESTING] Writting nodes >> :StackOverflowPost')
    stackoverflow_questions_processed.write\
        .format("org.neo4j.spark.DataSource")\
        .mode("overwrite")\
        .option("labels", ":StackOverflowPost")\
        .option("node.keys", "id")\
        .save()
    
