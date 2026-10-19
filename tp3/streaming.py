# streaming.py
# ------------------------------------------------------------
# Consommateur Spark Streaming :
#   1. Lit le flux socket
#   2. Parse le CSV
#   3. Calcule le CA par produit sur une fenêtre de 10 secondes
#   4. Affiche le résultat dans la console
# ------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, window, sum as _sum

# ---------- 1. Session Spark ----------
spark = (SparkSession.builder
         .appName("TP3-Socket-Streaming")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- 2. Lecture du flux socket ----------
lines = (spark.readStream
         .format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# ---------- 3. Parsing du CSV ----------
# Format reçu : id,event_time,produit,quantite,prix_unitaire,region
sales = (lines
    .select(split(col("value"), ",").alias("c"))
    .select(
        col("c")[0].cast("int").alias("id"),
        col("c")[1].cast("timestamp").alias("event_time"),
        col("c")[2].alias("produit"),
        col("c")[3].cast("int").alias("quantite"),
        col("c")[4].cast("double").alias("prix_unitaire"),
        col("c")[5].alias("region"),
    )
    .withColumn("ca", col("quantite") * col("prix_unitaire"))
    .filter(col("region") == "Nord"))

# ---------- 4. Agrégation par fenêtre de 10 secondes ----------
result = (sales
    .groupBy(window(col("event_time"), "10 seconds"), col("produit"))
    .agg(_sum("ca").alias("ca_total"))
    .select(
        col("window.start").alias("debut"),
        col("window.end").alias("fin"),
        col("produit"),
        col("ca_total"),
    ))

# ---------- 5. Affichage console ----------
query = (result.writeStream
         .outputMode("complete")          # ← à faire varier dans le TP
         .format("console")
         .option("truncate", False)
         .start())

query.awaitTermination()