HIVE PART

CREATE EXTERNAL TABLE fine_food_reviews (
  Id INT,
  ProductId STRING,
  UserId STRING,
  ProfileName STRING,
  HelpfulnessNumerator INT,
  HelpfulnessDenominator INT,
  Score INT,
  `Time` BIGINT,
  Summary STRING,
  Text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://finefood25/fine_food_reviews/';

---------------------------------------------------------------------------------------------------------------------
Pyspark Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc

def main():
    # Creating a Spark session with Hive support
    spark = SparkSession.builder \
        .appName("FineFoodReviewsAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Reading data from the external Hive table
    reviews_df = spark.sql("SELECT * FROM fine_food_reviews")
    
    # Cleaning the data: filter out rows with null Summary or invalid Score values (Score between 1 and 5)
    cleaned_df = reviews_df.filter("Summary IS NOT NULL AND Score >= 1 AND Score <= 5")
    
    # Aggregating data: calculate average score per product and get top 10 products with highest average score
    product_avg = cleaned_df.groupBy("ProductId") \
        .agg(avg("Score").alias("AverageScore")) \
        .orderBy(desc("AverageScore"))
    print("Top 10 Products by Average Score:")
    product_avg.show(10)
    
    # Additional Analysis: Distribution of review scores
    print("Review Score Distribution:")
    score_dist = spark.sql("""
        SELECT Score, COUNT(*) AS ReviewCount 
        FROM fine_food_reviews 
        GROUP BY Score 
        ORDER BY Score
    """)
    score_dist.show()
    
    spark.stop()

if __name__ == "__main__":
    main()

----------------------------------------------------------------------------------------------------------------------------------
Task 4 Pysparkjob

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PySparkJob")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Updated_Pipeline") \
        .enableHiveSupport() \
        .getOrCreate()

    logger.info("Spark session initialized.")

    # Load data from Hive
    df = spark.sql("SELECT * FROM fine_food_reviews")
    logger.info("Data loaded successfully.")

    # Display schema and preview
    df.printSchema()
    df.show(5)

    # Check for null values
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c + "_nulls") for c in df.columns])
    null_counts.show()
    
    # Handle missing values
    df = df.na.fill({"helpfulnessnumerator": 0, "helpfulnessdenominator": 1, "time": 0})
    logger.info("Missing values handled.")

    # Create binary labels based on score
    df = df.withColumn("label", when(col("score") >= 4, 1).otherwise(0))

    # Assemble feature vector while handling invalid values
    assembler = VectorAssembler(
        inputCols=["helpfulnessnumerator", "helpfulnessdenominator", "time"],
        outputCol="features",
        handleInvalid="skip"
    )
    assembled = assembler.transform(df)
    
    # Split data for training and testing
    train_data, test_data = assembled.randomSplit([0.8, 0.2], seed=42)

    # Train Random Forest model
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50, maxDepth=10)
    model = rf.fit(train_data)
    logger.info("Model training completed.")

    # Generate predictions
    predictions = model.transform(test_data)

    # Convert features to string before saving
    predictions = predictions.withColumn("features", col("features").cast("string"))

    predictions.select("features", "label", "prediction").show(5)

    # Evaluate accuracy
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    # Log accuracy to ensure it's visible in YARN logs
    logger.info(f"Test Accuracy: {accuracy}")
    print(f"Test Accuracy: {accuracy}")  # Ensure visibility in CLI

    # **SAVE OUTPUT TO GCS**
    predictions.select("features", "label", "prediction").write.mode("overwrite").csv("gs://finefood25/spark_output")

    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()

-----------------------------------------------------------------------------------------------------------------------------
logs of pyspark

INFO:PySparkJob:Data loaded successfully.
root
 |-- id: integer (nullable = true)
 |-- productid: string (nullable = true)
 |-- userid: string (nullable = true)
 |-- profilename: string (nullable = true)
 |-- helpfulnessnumerator: integer (nullable = true)
 |-- helpfulnessdenominator: integer (nullable = true)
 |-- score: integer (nullable = true)
 |-- time: long (nullable = true)
 |-- summary: string (nullable = true)
 |-- text: string (nullable = true)

25/06/17 11:55:42 INFO SQLStdHiveAccessController: Created SQLStdHiveAccessController for session context : HiveAuthzSessionContext [sessionString=8633aa1b-63b9-46de-843e-b86ec44cb4fe, clientType=HIVECLI]
25/06/17 11:55:42 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
25/06/17 11:55:42 INFO metastore: Mestastore configuration hive.metastore.filter.hook changed from org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl to org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook
25/06/17 11:55:42 INFO metastore: Closed a connection to metastore, current connections: 0
25/06/17 11:55:42 INFO metastore: Trying to connect to metastore with URI thrift://varni25-m:9083
25/06/17 11:55:42 INFO metastore: Opened a connection to metastore, current connections: 1
25/06/17 11:55:42 INFO metastore: Connected to metastore.
25/06/17 11:55:44 INFO GhfsStorageStatistics: Detected potential high latency for operation op_list_status. latencyMs=568; previousMaxLatencyMs=0; operationCount=1; context=gs://finefood25/fine_food_reviews
25/06/17 11:55:44 INFO FileInputFormat: Total input files to process : 1
+----+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
|  id| productid|        userid|         profilename|helpfulnessnumerator|helpfulnessdenominator|score|      time|             summary|                text|
+----+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
|null| ProductId|        UserId|         ProfileName|                null|                  null| null|      null|             Summary|                Text|
|   1|B001E4KFG0|A3SGXH7AUHU8GW|          delmartian|                   1|                     1|    5|1303862400|Good Quality Dog ...|I have bought sev...|
|   2|B00813GRG4|A1D87F6ZCVE5NK|              dll pa|                   0|                     0|    1|1346976000|   Not as Advertised|"Product arrived ...|
|   3|B000LQOCH0| ABXLMWJIXXAIN|"Natalia Corres "...|                   1|                     1|    4|1219017600|"""Delight"" says...|"This is a confec...|
|   4|B000UA0QIQ|A395BORC6FGVXV|                Karl|                   3|                     3|    2|1307923200|      Cough Medicine|If you are lookin...|
+----+----------+--------------+--------------------+--------------------+----------------------+-----+----------+--------------------+--------------------+
only showing top 5 rows

25/06/17 11:55:50 INFO GhfsStorageStatistics: Detected potential high latency for operation op_list_status. latencyMs=591; previousMaxLatencyMs=568; operationCount=2; context=gs://finefood25/fine_food_reviews
25/06/17 11:55:50 INFO FileInputFormat: Total input files to process : 1
+--------+---------------+------------+-----------------+--------------------------+----------------------------+-----------+----------+-------------+----------+
|id_nulls|productid_nulls|userid_nulls|profilename_nulls|helpfulnessnumerator_nulls|helpfulnessdenominator_nulls|score_nulls|time_nulls|summary_nulls|text_nulls|
+--------+---------------+------------+-----------------+--------------------------+----------------------------+-----------+----------+-------------+----------+
|       1|              0|           0|                0|                      4171|                        1091|        314|         4|            0|         0|
+--------+---------------+------------+-----------------+--------------------------+----------------------------+-----------+----------+-------------+----------+

INFO:PySparkJob:Missing values handled.
25/06/17 11:56:09 INFO FileInputFormat: Total input files to process : 1
25/06/17 11:56:25 INFO FileInputFormat: Total input files to process : 1
25/06/17 11:57:18 WARN DAGScheduler: Broadcasting large task binary with size 1201.8 KiB
25/06/17 11:57:24 WARN DAGScheduler: Broadcasting large task binary with size 1948.7 KiB
25/06/17 11:57:31 WARN DAGScheduler: Broadcasting large task binary with size 3.0 MiB
INFO:PySparkJob:Model training completed.
25/06/17 11:57:41 INFO FileInputFormat: Total input files to process : 1
25/06/17 11:57:41 WARN DAGScheduler: Broadcasting large task binary with size 1160.8 KiB
+--------------------+-----+----------+
|            features|label|prediction|
+--------------------+-----+----------+
|[0.0,0.0,1.346976E9]|    0|       1.0|
|[0.0,0.0,1.342051...|    1|       1.0|
|[0.0,0.0,1.336003...|    1|       1.0|
|[1.0,1.0,1.339545...|    0|       1.0|
|[0.0,0.0,1.324598...|    1|       1.0|
+--------------------+-----+----------+
only showing top 5 rows

25/06/17 11:57:45 INFO FileInputFormat: Total input files to process : 1
25/06/17 11:57:45 WARN DAGScheduler: Broadcasting large task binary with size 1180.3 KiB
INFO:PySparkJob:Test Accuracy: 0.8097964432621927
Test Accuracy: 0.8097964432621927