from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, regexp_extract
from dotenv import load_dotenv
import os

# def make_classifier():
#     clf = CSOClassifier(modules="semantic", enhancement="first", explanation=False, fast_classification=True, silent=True)
#     clf.setup()
#     return clf

# # local singleton
# _classifier = None

  # @pandas_udf(ArrayType(StringType()))
    # def classify_domains_pd(title: pd.Series, abstract: pd.Series, keywords: pd.Series) -> pd.Series:
    #     global _classifier
    #     if _classifier is None:
    #         _classifier = make_classifier()

    #     results = []
    #     texts_to_process = []
    #     for t, a, k in zip(title.fillna(''), abstract.fillna(''), keywords.fillna('')):
    #         text_parts = [t, a, k]
    #         texts_to_process.append("\n".join(filter(None, text_parts)))

    #     for text in texts_to_process:
    #         try:
    #             cleaned_text = text.strip()
    #             if not cleaned_text:
    #                 results.append([])
    #                 continue
                
    #             result = _classifier.run(cleaned_text)
    #             domain_list = result.get("enhanced", [])
    #             results.append(domain_list[:5] if domain_list else [])
    #         except Exception as e:
    #             results.append([]) 
    #     return pd.Series(results)


    # scientific_works_df_prepared = scientific_works_df\
    #     .select('EID', 'Title', 'Abstract', 'Author Keywords')\
    #     .repartition(8)
    # scientific_works_df = scientific_works_df_prepared\
    #     .withColumn(
    #     "Domain", classify_domains_pd("Title", "Abstract", "Author Keywords")
    # )


def normalize_column_names(df):
    new_names = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df.toDF(*new_names)


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

def write_csv(df, year=None):
    df  \
        .select('EID', 'Title', 'Abstract', 'Author Keywords')\
        .filter( True if year == None else col('Year') == year) \
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .option("quoteAll", True) \
        .csv('../../data/silver_layer/scopus/preprocessed_papers/all_years_preprocessed' if year == None else f"../../data/silver_layer/scopus/preprocessed_papers/{year}_preprocessed")

if __name__ == "__main__":
    spark = init_spark()

    scopus_dataset = spark.read\
                    .option("recursiveFileLookup", "true")\
                    .option("inferSchema", "true")\
                    .option("header", "true")\
                    .csv("../../data/bronze_layer/scopus/",  
                         quote='"',
                        escape='"',
                        multiLine=True)
    
    scopus_dataset = normalize_column_names(scopus_dataset)

    # scientific_works_df = scopus_dataset.select('eid', 'title', 'year',
    #                                             'doi', 'document_type', 'source', 
    #                                             'publication_stage', 'cited_by', 'open_access', 
    #                                             'abstract', 'author_keywords')
    

    # paper_years_df = scientific_works_df.select('Year').distinct()
    # years = [row['Year'] for row in paper_years_df.collect()]

    # for year in years:
    #     print(f"Writing preprocessed file for year: {year}...")
    #     write_csv(scientific_works_df, year = year)
    
    # print('Writting nodes >> :Paper')

    # scientific_works_df.drop('Author keywords', 'Abstract').write \
    #                             .format("org.neo4j.spark.DataSource")\
    #                             .mode("overwrite")\
    #                             .option("labels", ":Paper")\
    #                             .option("node.keys", "eid")\
    #                             .save()



    scopus_dataset = scopus_dataset.select('eid', 'authors', 'authors_with_affiliations').filter(col("authors_with_affiliations").isNotNull()).persist()

    authors_df = scopus_dataset \
        .select('eid', split(col("authors"), ";").alias("author_list")) \
        .withColumn("author", explode("author_list")) \
        .withColumn("author", trim(col("author"))) \
        .filter(col("author").isNotNull())
    
    print('Writting nodes >> :Author')

    # authors_df.select("author").distinct()\
    #     .write \
    #     .format("org.neo4j.spark.DataSource") \
    #     .mode("overwrite") \
    #     .option("labels", ":Author") \
    #     .option("node.keys", "author") \
    #     .option("batch.size", "5000") \
    #     .save()
    
    print('Writting relationship >> :Author -[:AUTHORED]-> :Paper')
    # authors_df.select("author", "eid") \
    #     .write \
    #     .format("org.neo4j.spark.DataSource") \
    #     .mode("overwrite") \
    #     .option("relationship", "AUTHORED") \
    #     .option("relationship.save.strategy", "keys") \
    #     .option("relationship.source.labels", ":Author") \
    #     .option("relationship.source.node.keys", "author:author") \
    #     .option("relationship.target.labels", ":Paper") \
    #     .option("relationship.target.node.keys", "eid:eid") \
    #     .option("batch.size", "5000") \
    #     .save()

    # Extract (author, affiliation) pairs from authors_with_affiliations
    author_aff_df = scopus_dataset \
        .select("authors_with_affiliations") \
        .filter(col("authors_with_affiliations").isNotNull()) \
        .withColumn("entry", explode(split(col("authors_with_affiliations"), ";"))) \
        .withColumn("entry", trim(col("entry"))) \
        .withColumn("author", regexp_extract("entry", r"^([^,]+)", 1)) \
        .withColumn("affiliation", regexp_extract("entry", r"^[^,]+,\s*(.*)", 1)) \
        .filter((col("author") != "") & (col("affiliation") != "")) \
        .select("author", "affiliation") \
        .distinct() \
        .persist()
    
    print('Writting nodes >> :Affiliation')

    author_aff_df.select("affiliation").write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("labels", ":Affiliation") \
        .option("node.keys", "affiliation") \
        .option("batch.size", "5000") \
        .save()

    print('Writting relationship >> :Author -[:AFFILIATED_WITH]-> :Affiliation')

    author_aff_df.select("author", "affiliation").write \
        .format("org.neo4j.spark.DataSource") \
        .mode("overwrite") \
        .option("relationship", "AFFILIATED_WITH") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Author") \
        .option("relationship.source.node.keys", "author:author") \
        .option("relationship.target.labels", ":Affiliation") \
        .option("relationship.target.node.keys", "affiliation:affiliation") \
        .option("batch.size", "5000") \
        .save()
    
    scopus_dataset.unpersist()
    author_aff_df.unpersist()
