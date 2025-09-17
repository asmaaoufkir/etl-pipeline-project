from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Input file path")
    parser.add_argument("--es-index", default="logstats", help="Elasticsearch index name")
    args = parser.parse_args()
    
    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName("AdvancedETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()
    
    # Schéma pour les logs
    log_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("level", StringType(), True),
        StructField("application", StringType(), True),
        StructField("message", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("response_time", DoubleType(), True),
        StructField("error_code", IntegerType(), True)
    ])
    
    # Lecture des données
    df = spark.read.schema(log_schema).json(args.input)
    
    # Nettoyage et transformation
    df_clean = df.filter(col("timestamp").isNotNull()) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("is_error", when(col("level") == "ERROR", 1).otherwise(0))
    
    # Agrégations pour analyses
    stats_by_app = df_clean.groupBy("application", "hour") \
        .agg(
            count("*").alias("total_requests"),
            avg("response_time").alias("avg_response_time"),
            sum("is_error").alias("total_errors")
        ) \
        .withColumn("error_rate", col("total_errors") / col("total_requests"))
    
    # Machine Learning: clustering des applications par performance
    feature_cols = ["avg_response_time", "error_rate"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    ml_df = assembler.transform(stats_by_app)
    
    kmeans = KMeans(k=3, seed=42)
    model = kmeans.fit(ml_df)
    clustered_df = model.transform(ml_df)
    
    # Écriture des résultats dans Elasticsearch
    clustered_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", args.es_index) \
        .mode("overwrite") \
        .save()
    
    # Affichage des résultats
    print("Résumé de l'analyse:")
    clustered_df.show(10)
    
    spark.stop()

if __name__ == "__main__":
    main()
