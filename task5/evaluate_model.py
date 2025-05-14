from pyspark.sql.functions import col

def evaluate_model(model, testData):
    """
    Tính precision và recall theo công thức thủ công (TP, FP, FN).

    Args:
        model: Trained model
        testData: DataFrame chứa test data

    Returns:
        (precision, recall)
    """
    predictions = model.transform(testData)

    # TP: label = 1, prediction = 1
    tp = predictions.filter((col("label") == 1) & (col("prediction") == 1)).count()
    # FP: label = 0, prediction = 1
    fp = predictions.filter((col("label") == 0) & (col("prediction") == 1)).count()
    # FN: label = 1, prediction = 0
    fn = predictions.filter((col("label") == 1) & (col("prediction") == 0)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    return precision, recall
