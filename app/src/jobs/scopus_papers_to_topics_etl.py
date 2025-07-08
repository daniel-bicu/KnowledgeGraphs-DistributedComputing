from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import StringType, StructType, StructField
from dotenv import load_dotenv
import os


load_dotenv()
neo4j_url = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
neo4j_db = os.getenv("NEO4J_PASSWORD")

def init_spark(config=None):
    spark = SparkSession.builder\
        .appName(name='ScopusJobProcessor')\
        .config("spark.sql.shuffle.partitions", '10') \
        .master("local[*]")\
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.6_for_spark_3") \
        .config("neo4j.url", neo4j_url) \
        .config("neo4j.authentication.type", "basic") \
        .config("neo4j.authentication.basic.username", neo4j_user) \
        .config("neo4j.authentication.basic.password", neo4j_pwd) \
        .config("neo4j.authentication.basic.password", neo4j_db) \
        .getOrCreate()

    
    return spark

spark = init_spark()
scientific_works_cso_added = spark.read \
                                    .option("recursiveFileLookup", "true")\
                                    .option("header", "true")\
                                    .schema(StructType(
                                            [StructField("eid", StringType()),
                                                StructField("result_cso", StringType())
                                            ]
                                    ))\
                                    .csv('../../data/gold_layer/scopus/classified_papers')
                                    
scientific_works_cso_classified_lookup = scientific_works_cso_added.withColumn("result_cso", split("result_cso", ','))
scientific_works_cso_classified_lookup = scientific_works_cso_classified_lookup \
                                    .withColumn("domain", explode(col("result_cso")))\
                                    .drop("result_cso")


print('Writting relationship >> :Paper -[:ADDRESSES_TOPIC]-> :Topic')
scientific_works_cso_classified_lookup.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("overwrite") \
    .option("relationship", "ADDRESSES_TOPIC") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":Paper") \
    .option("relationship.source.node.keys", "eid:eid") \
    .option("relationship.target.labels", ":Topic") \
    .option("relationship.target.node.keys", "domain:name") \
    .save()