package de.tuberlin.cit.pserver

import java.io.{File, PrintWriter}
import java.nio.charset.Charset
import java.nio.file.Paths

/**
  * This Flink job is used to preprocess the Criteo dataset
  * (http://labs.criteo.com/downloads/download-terabyte-click-logs/)
  *
  * The dataset itself has the following structure:
  *
  * Label - Target variable that indicates if an ad was clicked (1) or not (0).
  * I1-I13 - A total of 13 columns of integer features (mostly count features).
  * C1-C26 - A total of 26 columns of categorical features. The values of these features have been
  * hashed onto 32 bits for anonymization purposes.
  *
  * The following preprocessing steps are applied to the dataset:
  *
  * 1. Remove mean and scale integer features I1-I13 to unit variance
  * 2. One-hot-encode categorial features C1-C26 (supplied by parameter {numFeatures})
  *
  * Commandline Parameters:
  * --input <input path>
  * --output <output path>
  * --numFeatures <dimension of the feature vector for one-hot-encoding (defaults to 1048576)>
  * --mean <path to file the mean of the training data will be written>
  * --stdDeviation <path to file the std. deviation of the training data will be written>
  *
  * --testData
  *   if testData is set, additionally these parameters are required to read mean and std deviation
  *   computed from the training data:
  *   --mean <path to file containing the mean of the training data>
  *   --stdDeviation <path to file containing the std. deviation of the training data>
  * Labels and data are stored in seperate directories ("/label" and "/data") in the output directory
*/


object CriteoPreprocessingJob {
  val Seed = 0

  val NUM_LABELS = 1
  val NUM_INTEGER_FEATURES = 13
  val NUM_CATEGORIAL_FEATURES = 26
  val NUM_FEATURES = NUM_LABELS + NUM_INTEGER_FEATURES + NUM_CATEGORIAL_FEATURES


  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val parameters = ParameterTool.fromArgs(args)

    val inputPath = parameters.getRequired("input")
    val outputPath = parameters.getRequired("output")
    val numFeatures = parameters.getInt("numFeatures", math.pow(2, 20).toInt)
    val isTest = parameters.getBoolean("testData", false)

    if (isTest) {
      require(parameters.get("mean").nonEmpty, "mean vector must be provided if --test is set")
      require(parameters.get("stdDeviation").nonEmpty, "std. dev. vector must be provided if --test is set")
    }
    val meanPath = parameters.get("mean")
    val stdDeviationPath = parameters.get("stdDeviation")


    val input = readInput(env, inputPath).zipWithIndex

    val labels = input.map(input => (input._1, input._2._1))
      .map{rowLabel => (rowLabel._1, if (rowLabel._2 == 1) 1 else -1)}

    val nSamples = input.count()

    // compute mean for integer features or read from file (if test data)
    val mean =
      if (isTest) {
        val meanFile = Source.fromFile(meanPath)
        val mean = env.fromCollection(meanFile.getLines().map(_.split(",")).map(_.map(_.toDouble)).toSeq)
        meanFile.close()
        mean
      } else {
        val sum = input.map(input => input._2._2).reduce((integerLeft, integerRight) =>
          (integerLeft, integerRight).zipped map (_ + _))

        sum.map(_.map(_.toDouble / nSamples))
      }

    // compute standard deviation for integer features or read from file (if test data)
    val stdDeviation =
      if (isTest) {
        val stdDeviationFile = Source.fromFile(stdDeviationPath)
        val stdDeviation = env.fromCollection(stdDeviationFile.getLines().map(_.split(",")).map(_.map(_.toDouble)).toSeq)
        stdDeviationFile.close()
        stdDeviation
      } else {
        val deviation = input.map(input => input._2._2).map(new RichMapFunction[Array[Int], Array[Double]]() {
          var mean: Array[Double] = null

          override def open(config: Configuration): Unit = {
            mean = getRuntimeContext.getBroadcastVariable[Array[Double]]("mean").asScala.head
          }

          def map(integerFeature: Array[Int]): Array[Double] = {
            (integerFeature, mean).zipped map ((feature, mean) => math.pow(feature - mean, 2))
          }
        }).withBroadcastSet(mean, "mean")
          .reduce((stdDevLeft, stdDevRight) => (stdDevLeft, stdDevRight).zipped map (_ + _))

        deviation.map(_.map(dev => math.sqrt(dev / nSamples)))
      }

    /*
    remove mean and scale to unit variance (standard deviation), hash features onto numFeatures
    dimensions
    */

    val transformedFeatures = input.map(x => (x._1, (x._2._2, x._2._3)))
      .flatMap(new RichFlatMapFunction[(Long, (Array[Int], Array[String])), (Long, Long, Double)]() {
      var mean: Array[Double] = null
      var variance: Array[Double] = null

      override def open(config: Configuration): Unit = {
        mean = getRuntimeContext.getBroadcastVariable[Array[Double]]("mean").asScala.head
        variance = getRuntimeContext.getBroadcastVariable[Array[Double]]("stdDeviation").asScala.head
      }

      def flatMap(in: (Long, (Array[Int], Array[String])), collector: Collector[(Long, Long, Double)]) = {
        val row = in._1
        val integerFeatures = in._2._1
        val categorialFeatures = in._2._2

        for (col <- 0 until NUM_INTEGER_FEATURES) {
          collector.collect((row, col, (integerFeatures(col) - mean(col)) / variance(col)))
        }

        val hashedIndices = categorialFeatures
          .filter(!_.isEmpty)
          .map(feature => {
            murmurHash(feature, 1, numFeatures)
          })
          .groupBy(_._1).map(colCount => (colCount._1 + NUM_INTEGER_FEATURES, colCount._2.map(_._2).sum))
          .filter(_._2 != 0)

        hashedIndices.foreach { feature =>
          collector.collect((row, feature._1, feature._2))
        }
      }
    }).withBroadcastSet(mean, "mean").withBroadcastSet(stdDeviation, "stdDeviation")

    val (labelsName, dataName) = if (isTest) ("labels_test", "data_test") else ("labels_train", "data_train")

    labels.map(l => (l._1, 0, l._2)).writeAsCsv(Paths.get(outputPath, labelsName).toString, writeMode = WriteMode.OVERWRITE)
    transformedFeatures.writeAsCsv(Paths.get(outputPath, dataName).toString, writeMode = WriteMode.OVERWRITE)

    // execute program
    env.execute("Criteo Preprocessing")

    println("mean: " + mean.collect().head.mkString(","))
    println("stdDeviation: " + stdDeviation.collect().head.mkString(","))

    if (! isTest) {
      val meanWriter = new PrintWriter(new File(meanPath))
      meanWriter.println(mean.collect().head.mkString(","))
      meanWriter.close()

      val stdDevWriter = new PrintWriter(new File(stdDeviationPath))
      stdDevWriter.println(stdDeviation.collect().head.mkString(","))
      stdDevWriter.close()
    }
  }

  def readInput(env: ExecutionEnvironment, input: String, delimiter: String = "\t") = {
    env.readTextFile(input) map { line =>
      val features = line.split(delimiter, -1)

      val label = features.take(NUM_LABELS).head.toInt
      val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
        .map(string => if (string.isEmpty) 0 else string.toInt)
      val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)

      (label, integerFeatures, categorialFeatures)
    }
  }

  private def murmurHash(feature: String, count: Int, numFeatures: Int): (Int, Int) = {
    val hash = MurmurHash3.bytesHash(feature.getBytes(Charset.forName("UTF-8")), Seed)
    val index = scala.math.abs(hash) % numFeatures
    /* instead of using two hash functions (Weinberger et al.), assume the sign is in-
       dependent of the other bits */
    val value = if (hash >= 0) count else -1 * count
    (index, value)
  }
}
