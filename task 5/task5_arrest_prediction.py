import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
    parser = argparse.ArgumentParser(description="Arrest Prediction using Spark ML")
    parser.add_argument("--parquet", required=True, help="Path to input Parquet file")
    parser.add_argument("--output", required=True, help="Path to output CSV result")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ArrestPrediction").getOrCreate()

    # Step 1: Load and filter data
    df = spark.read.parquet(args.parquet)
    df = df.filter((col("Arrest") == "true") | (col("Arrest") == "false"))
    df = df.select("PrimaryType", "Description", "Arrest")

    # Step 2: Combine PrimaryType and Description
    df = df.withColumn("text", concat_ws(" ", col("PrimaryType"), col("Description")))

    # Step 3: Machine Learning Pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=1000)
    labelIndexer = StringIndexer(inputCol="Arrest", outputCol="label")
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    pipeline = Pipeline(stages=[tokenizer, hashingTF, labelIndexer, lr])

    # Step 4: Train-test split
    trainingData, testData = df.randomSplit([0.8, 0.2], seed=42)

    # Step 5: Training
    start_time = time.time()
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)
    end_time = time.time()
    total_time = end_time - start_time

    # Step 6: Evaluation
    evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")

    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)

    print(f"Total training + prediction time: {total_time:.2f} seconds")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")

    # Step 7: Export prediction results
    output_df = predictions.select("PrimaryType", "Description", "Arrest", "label", "prediction")
    output_df.write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
