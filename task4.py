import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, lit

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Spatio-Temporal Analysis of Crimes")
    parser.add_argument('--parquet', required=True, help='Path to the input Parquet file')
    parser.add_argument('--start_date', required=True, help='Start date (MM/DD/YYYY)')
    parser.add_argument('--end_date', required=True, help='End date (MM/DD/YYYY)')
    parser.add_argument('--x_min', required=True, type=float, help='Minimum X-coordinate')
    parser.add_argument('--y_min', required=True, type=float, help='Minimum Y-coordinate')
    parser.add_argument('--x_max', required=True, type=float, help='Maximum X-coordinate')
    parser.add_argument('--y_max', required=True, type=float, help='Maximum Y-coordinate')
    parser.add_argument('--output', required=True, help='Path to save output CSV')

    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder.appName("CrimeSpatioTemporalAnalysis").getOrCreate()

    # Load the dataset from Parquet
    df = spark.read.parquet(args.parquet)

    # Parse the date columns
    df = df.withColumn('date', to_timestamp(df['date'], 'MM/dd/yyyy hh:mm:ss a'))
    start_date = to_date(lit(args.start_date), 'MM/dd/yyyy')
    end_date = to_date(lit(args.end_date), 'MM/dd/yyyy')

    # Filter the dataframe based on date range and coordinates
    filtered_df = df.filter(
        (df['date'] >= start_date) &
        (df['date'] <= end_date) &
        (df['XCoordinate'] >= args.x_min) &
        (df['XCoordinate'] <= args.x_max) &
        (df['YCoordinate'] >= args.y_min) &
        (df['YCoordinate'] <= args.y_max)
    )

    # Select relevant columns
    result_df = filtered_df.select('XCoordinate', 'YCoordinate', 'CaseNumber', 'date')

    # Save the result to a CSV file
    result_df.write.option("header", "true").csv(os.path.join(args.output, "RangeReportResult.csv"))

    print(f"✅ Results saved to: {os.path.join(args.output, 'RangeReportResult.csv')}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
