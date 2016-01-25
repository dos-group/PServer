import java.io.File

import CriteoPreprocessingJobTest.{buildJobArguments, meanFile, numFeatures, outputTrain,
outputTest, stdDevFile, tmpDir, readLibsvmFile, DELTA}
import de.cit.pserver.CriteoPreprocessingJob
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.io.Source

case class DataPoint(label: Double, features:Seq[(Int, Double)])
object DataPoint {
  def apply(string: String): DataPoint = {
    val entries = string.split(" ")
    val label = entries.head.toDouble
    val features = entries.tail.map(_.split(":")).map(entry => (entry.head.toInt, entry.tail.head.toDouble))
    new DataPoint(label, features)
  }
}

class CriteoPreprocessingJobTest extends FlatSpec with Matchers with Inspectors {
  "Criteo preprocessing train" should "do mean removal, scaling to unit variance and feature" +
    "hashing of the categorial features" in  {

    CriteoPreprocessingJob.main(buildJobArguments(
      getClass.getResource("/criteo_train_testcase.tsv").getFile,
      tmpDir,
      numFeatures,
      meanFile,
      stdDevFile,
      isTestData=false
    ))

    new File(outputTrain).exists() should equal(true)

    val result = readLibsvmFile(outputTrain)
    val expectedResult = readLibsvmFile(getClass.getResource("/criteo_train.libsvm").getFile)

    testDatapointEquality(result, expectedResult)
  }

  "Criteo preprocessing test" should "do mean removal, scaling to unit variance (both read from" +
    "file) and feature hashing of the categorial features" in {

    CriteoPreprocessingJob.main(buildJobArguments(
      getClass.getResource("/criteo_test_testcase.tsv").getFile,
      tmpDir,
      numFeatures,
      meanFile,
      stdDevFile,
      isTestData=true
    ))

    new File(outputTest).exists() should equal(true)

    val result = readLibsvmFile(outputTest)
    val expectedResult = readLibsvmFile(getClass.getResource("/criteo_test.libsvm").getFile)

    testDatapointEquality(result, expectedResult)
  }

  def testDatapointEquality(result: Seq[DataPoint], expectedResult: Seq[DataPoint]) = {
    for ((expected, computed) <- expectedResult zip result) {
      expected.label should equal(computed.label)
      for((expEntry, compEntry) <- expected.features zip computed.features) {
        expEntry._1 should equal(compEntry._1)
        expEntry._2 should equal(compEntry._2 +- DELTA)
      }
    }
  }
}

object CriteoPreprocessingJobTest {
  val tmpDir = "/tmp/"
  val meanFile = tmpDir + "mean.csv"
  val stdDevFile = tmpDir + "stdDev.csv"

  val outputTrain = tmpDir + "criteo_train.libsvm"
  val outputTest = tmpDir + "criteo_test.libsvm"

  val numFeatures = math.pow(2, 20).toInt

  val DELTA = 1e-9

  def buildJobArguments(inputPath: String, outputPath: String, numFeatures: Int, meanFile: String,
                        stdDevFile: String, isTestData: Boolean=false): Array[String] = {
    Array("--input", inputPath,
          "--output", outputPath,
          "--numFeatures", numFeatures.toString,
          "--mean", meanFile,
          "--stdDeviation", stdDevFile,
          "--testData", isTestData.toString)
  }

  def readLibsvmFile(file: String): Seq[DataPoint] = {
    Source
      .fromFile(file)
      .getLines()
      .map(DataPoint(_))
      .toSeq
  }
}
