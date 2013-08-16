package spark.mllib.util

import scala.util.Random
import scala.math.signum

import spark.{RDD, SparkContext}

import org.jblas.DoubleMatrix
import spark.mllib.regression.LabeledPoint

/**
 * Generate sample data used for SVM. This class generates uniform random values
 * for the features and adds Gaussian noise with weight 0.1 to generate labels.
 */
object SVMDataGenerator {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SVMGenerator " +
        "<master> <output_dir> [num_examples] [num_features] [num_partitions]")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 2
    val parts: Int = if (args.length > 4) args(4).toInt else 2

    val sc = new SparkContext(sparkMaster, "SVMGenerator")

    val globalRnd = new Random(94720)
    val trueWeights = new DoubleMatrix(1, nfeatures+1,
      Array.fill[Double](nfeatures + 1) { globalRnd.nextGaussian() }:_*)

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new Random(42 + idx)

      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }
      val y = signum((new DoubleMatrix(1, x.length, x:_*)).dot(trueWeights) + rnd.nextGaussian() * 0.1)
      LabeledPoint(y, x)
    }

    MLUtils.saveLabeledData(data, outputPath)
    sc.stop()
  }
}
