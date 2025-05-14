def export_predictions(predictions, output_path):
    """
    Exports the predictions to a CSV file.

    Args:
        predictions: DataFrame containing prediction results
        output_path: Path to save the output CSV
    """
    output_df = predictions.select("Primary Type", "Description", "Arrest", "label", "prediction")
    output_df.write.mode("overwrite").option("header", "true").csv(output_path)


