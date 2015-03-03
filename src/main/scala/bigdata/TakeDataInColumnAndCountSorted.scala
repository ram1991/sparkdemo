package bigdata

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kehinde on 15-03-03.
 */
object TakeDataInColumnAndCountSorted {

  // Specify the path to your data file
  val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

  val sc = new SparkContext(conf)

  def sparkJob() = {

    //load CSV
    val inputData=loadCSV("CrimesData.csv");

    //Take data in rows
    val batchInputData=takeDataInRowByBatch(inputData,5).persist()

    //Remove header column
    val batchInputDataWithoutHeader = dropHeader(batchInputData).persist()

    //Print data in column with counted values
    TakeColumnAndCountByValue(batchInputDataWithoutHeader, 5)

  }

  def loadCSV(csvFile:String): RDD[String]  = {

    sc.textFile(csvFile)

  }

  def takeDataInRowByBatch(data:RDD[String], lineNum:Int):RDD[String]= {

    data.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        parser.parseLine(line).mkString(",")
      }).take(lineNum)
    })
  }

  def TakeColumnAndCountByValue(data:RDD[String], ColumnNum:Int)= {

    data.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(ColumnNum)).mkString(",")
      })
    }).countByValue().toList.sortBy(-_._2).foreach(println)
  }

  def dropHeader(data: RDD[String]): RDD[String] = {

    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def main(args: Array[String]) = sparkJob()
}

