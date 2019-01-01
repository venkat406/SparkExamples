import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.ml.evaluation.RegressionEvaluator


object MLDiabetes
{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ML Example1").master("local").getOrCreate()
    // Prepare training data from a list of (label, features) tuples.

    // https://www.kaggle.com/chethan187/classification-model-using-logistic-regression/data
    val inputPath = "C:\\Users\\venka\\Documents\\Dataset\\diabetes.csv"
    val diabetesData = spark.read.format("csv").option("header", true)
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("nullValue", "").load(inputPath)
    import spark.sqlContext.implicits._
    val assembler = new VectorAssembler()
      .setInputCols(Array("Pregnancies","Glucose","BloodPressure","SkinThickness","BMI","Insulin","Age"))
      .setOutputCol("features")
    val diabetesDataTransformed = assembler.transform(diabetesData)

    // Split training and test datasets
    val diabetesDataSplit = diabetesDataTransformed.randomSplit(Array(0.7, 0.3), seed = 100L)

    val trainingData = diabetesDataSplit(0)
    val testData = diabetesDataSplit(1)
    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression().setLabelCol("Outcome").setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.01)

    // Print out the parameters, documentation, and any default values.
    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

    // We may set parameters using setter methods.


    // Model training
    val model1 = lr.fit(trainingData)

    println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    // Alternative way to change params ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(diabetesDataTransformed, paramMapCombined)
    println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

    // predictions
    val testPredictions = model2.transform(testData)
    testPredictions.show()

    testPredictions.select("prediction", "Outcome", "features").show(10)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("Outcome")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(testPredictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

  }
}
