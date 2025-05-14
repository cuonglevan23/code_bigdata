from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression


def create_and_train_model(df, model_type="logistic_regression"):
    """
    Creates and trains a model using the specified model type.

    Args:
        df: DataFrame containing processed data
        model_type: Model type to use ("logistic_regression" or "random_forest")

    Returns:
        Trained model
    """
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=1000)
    labelIndexer = StringIndexer(inputCol="Arrest", outputCol="label")

    # Choose the model type
    if model_type == "random_forest":
        model = RandomForestClassifier(featuresCol="features", labelCol="label")
    else:
        model = LogisticRegression(featuresCol="features", labelCol="label")

    pipeline = Pipeline(stages=[tokenizer, hashingTF, labelIndexer, model])

    # Train-test split
    trainingData, testData = df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    trained_model = pipeline.fit(trainingData)
    return trained_model, testData
