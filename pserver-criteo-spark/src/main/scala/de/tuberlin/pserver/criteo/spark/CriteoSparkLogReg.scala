package de.tuberlin.pserver.criteo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils

object CriteoSparkLogReg extends App {

  if (args.length != 2) {
    Console.err.println("Usage: <jar> inputPath outputPath")
    System.exit(-1)
  }

  val inputPath = args(0)
  //val outputPath = args(1)

  val sc = new SparkContext(new SparkConf().setAppName("pserver-experiments-spark"))

  // Load training data in LIBSVM format.
  val data = MLUtils.loadLibSVMFile(sc, inputPath)

  // Split data into training (60%) and test (40%).
  //val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
  //val training = splits(0).cache()
  //val test = splits(1)

  // Run training algorithm to build the model
  val model = LogisticRegressionWithSGD.train(data, 50)

  // Compute raw scores on the test set.
  //val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  //val prediction = model.predict(features)
  //(prediction, label)
  //}

  // Get evaluation metrics.
  //val metrics = new MulticlassMetrics(predictionAndLabels)
  //val precision = metrics.precision
  //println("Precision = " + precision)

  // Save and load model
  //model.save(sc, "myModelPath")
  //val sameModel = LogisticRegressionModel.load(sc, "myModelPath")
}