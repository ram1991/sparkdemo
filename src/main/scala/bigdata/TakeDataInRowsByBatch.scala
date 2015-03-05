package bigdata

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kehinde on 15-03-03.
 */


object TakeDataInRowsByBatch {

  // Specify the path to your data file
  val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

  val sc = new SparkContext(conf)

  def sparkJob() = {

    //load CSV
    val inputData=loadCSV("CrimesData.csv");

    //Take data in rows
    val batchInputData=takeDataInRowByBatch(inputData,5)

  }

  def loadCSV(csvFile:String): RDD[String]  = {

    sc.textFile(csvFile)

  }

  def takeDataInRowByBatch(data:RDD[String], lineNum:Int)= {

    data.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        parser.parseLine(line).mkString(",")
      })
    }).take(lineNum).foreach(println)

  }



  def main(args: Array[String]) = sparkJob()
}
