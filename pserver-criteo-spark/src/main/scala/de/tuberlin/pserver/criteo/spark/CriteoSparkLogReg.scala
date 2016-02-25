package de.tuberlin.pserver.criteo.spark

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

case class Config(inputPathTrain: String = "", inputPathTest: String = "", local: Boolean = false,
                  stepSize: Double = 1.0, numIterations: Int = 50, miniBatchSize: Int = 1)

/*
  This Spark job runs logistic regression on a given dataset (pre-split into training and test set)
  and computes the precision of the model

  Important notes:
    - input must be in libsvm format
    - labels must be {0, 1}
*/

object CriteoSparkLogReg {

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Spark Logistic Regression", "0.1")
      opt[String]("inputPathTrain") action { (x, c) =>
        c.copy(inputPathTrain = x) } text "path to the training data"
      opt[String]("inputPathTest") action { (x, c) =>
        c.copy(inputPathTest = x) } text "path to the test data"
      opt[Unit]("local") action { (_, c) =>
        c.copy(local = true) } text "setting this flag will execute the job locally"
      opt[Double]("stepSize") action { (x, c) =>
        c.copy(stepSize = x) } text "Gradient descent step size"
      opt[Int]("numIterations") action { (x, c) =>
        c.copy(numIterations = x) } text "Gradient descent iterations"
      opt[Int]("miniBatchSize") action { (x, c) =>
        c.copy(miniBatchSize = x) } text "Gradient descent batch size"
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val conf = new SparkConf().setAppName("pserver-experiments-spark-criteo")

        if (config.local) conf.setMaster("local[*]")

        val sc = new SparkContext(conf)

        // Load training data in LIBSVM format.
        val dataTrain = MLUtils.loadLibSVMFile(sc, config.inputPathTrain)
        val dataTest = MLUtils.loadLibSVMFile(sc, config.inputPathTest)

        // Run training algorithm to build the model
        val model = LogisticRegressionWithSGD.train(
          dataTrain, config.numIterations,config.stepSize, config.miniBatchSize
        )

        // Compute raw scores on the test set.
        val predictionAndLabels = dataTest.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val precision = metrics.precision
        println("Precision = " + precision)

      case None =>
        sys.exit(-1)
    }
  }
}
