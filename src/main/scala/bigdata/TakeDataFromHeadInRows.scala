package bigdata

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by Kehinde on 15-02-27.
 */
object TakeDataFromHeadInRows  {

  def sparkJob() = {
// Specify the path to your data file
val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

val sc = new SparkContext(conf)

// load the data
val inputData = sc.textFile("CrimesData.csv")


inputData.mapPartitions(lines => {
val parser = new CSVParser(',')
lines.map(line => {
parser.parseLine(line).mkString(",")
})
}).take(5).foreach(println)

}

def main(args: Array[String]) = sparkJob()

}





