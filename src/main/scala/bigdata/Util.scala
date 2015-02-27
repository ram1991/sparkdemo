package bigdata

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by Kehinde on 15-02-27.
 */
object DataUtil {

  def main(args: Array[String]) = {
    val appName: String = args(0)
    val inputFile: String = args(1)
    val lineNumber: Int = args(2).toInt
    val conf = new SparkConf().setAppName("appName")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    println("Loading data")
    val inputData = sc.textFile(inputFile).cache()
    println("Load data successfully")

    println("Printing result for the specified line numbers")
    inputData.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        parser.parseLine(line).mkString(",")
      })
    }).take(lineNumber).foreach(println)

  }
}





