import argparse
import time
from pyspark.sql import SparkSession
from task5.create_spark_session import create_spark_session
from task5.load_and_process_data import load_and_process_data
from task5.process_text_features import process_text_features
from task5.create_and_train_model import create_and_train_model
from task5.evaluate_model import evaluate_model
from task5.export_predictions import export_predictions


def run_experiment(spark, parquet_path, output_path, model_type):
    # Load and preprocess data
    df = load_and_process_data(spark, parquet_path)
    df = process_text_features(df)
    # Train and evaluate
    model, test_data = create_and_train_model(df, model_type=model_type)
    start = time.time()
    precision, recall = evaluate_model(model, test_data)
    duration = time.time() - start
    # Export predictions
    predictions = model.transform(test_data)
    export_predictions(predictions, output_path)
    # Sample
    sample = predictions.select("Primary Type", "Description", "Arrest", "label", "prediction").limit(5)

    return {
        "dataset": parquet_path,
        "time": duration,
        "precision": precision,
        "recall": recall,
        "sample_df": sample
    }


def main():
    parser = argparse.ArgumentParser(description="Run arrest-prediction experiments on multiple datasets")
    parser.add_argument("--parquets", nargs=2, required=True,
                        help="Paths to two Parquet files (10k and 100k)")
    parser.add_argument("--outputs", nargs=2, required=True,
                        help="Output CSV paths for two runs")
    parser.add_argument("--model", choices=["logistic_regression", "random_forest"],
                        default="logistic_regression", help="Model to use")
    args = parser.parse_args()

    spark = create_spark_session()
    results = []
    for parquet, output in zip(args.parquets, args.outputs):
        res = run_experiment(spark, parquet, output, args.model)
        results.append(res)
        print(f"\n--- Results for {parquet} ---")
        print(f"Time: {res['time']:.2f}s  Precision: {res['precision']:.4f}  Recall: {res['recall']:.4f}")
        res['sample_df'].show(truncate=False)

    # Summary table
    print("\n=== Summary ===")
    print(f"{'Dataset':40} {'Time(s)':>8} {'Precision':>10} {'Recall':>8}")
    for r in results:
        print(f"{r['dataset']:40} {r['time']:8.2f} {r['precision']:10.4f} {r['recall']:8.4f}")

    spark.stop()


if __name__ == "__main__":
    main()
