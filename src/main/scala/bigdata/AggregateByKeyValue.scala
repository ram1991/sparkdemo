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

  def sparkJob(crime:String) = {

    //load CSV
    val inputData=sc.textFile("CrimesData.csv");

    // split / clean data
    val headerAndRows = inputData.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val dataRDD= data.map(splits => header.zip(splits).toMap)

    val result = dataRDD.map(x=>(x("Description"))).countByValue()


  }

  def main(args: Array[String]) = sparkJob("FORCIBLE ENTRY")
}
