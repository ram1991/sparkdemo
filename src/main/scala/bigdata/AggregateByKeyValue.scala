package bigdata

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kehinde on 15-03-04.
 */
object AggregateByKeyValue {

  // Specify the path to your data file
  val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

  val sc = new SparkContext(conf)

  def sparkJob(value:String) = {

    //load CSV
    val inputData=loadCSV("CrimesData.csv");

    //Take data in rows
    val batchInputData=takeDataInRowByBatch(inputData,20).persist()

    //Remove header column
    val batchInputDataWithoutHeader = dropHeader(batchInputData).persist()

    //Pending...value aggregate


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


  def dropHeader(data: RDD[String]): RDD[String] = {

    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }



  def main(args: Array[String]) = sparkJob("DOMESTIC BATTERY SIMPLE")

}
