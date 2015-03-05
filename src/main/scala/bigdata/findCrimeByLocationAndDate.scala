package bigdata

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kehinde on 15-03-05.
 */
object findCrimeByLocationAndDate {  // Specify the path to your data file
val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

  val sc = new SparkContext(conf)

  def sparkJob(crime: String, location: String) = {

    //load CSV
    val inputData = sc.textFile("CrimesData.csv");

    // split / clean data
    val headerAndRows = inputData.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val dataRDD = data.map(splits => header.zip(splits).toMap)

    val result = dataRDD.map(x => (x("ID"), x("Date"), x("Block"), x("Description"), x("Location Description")))

    val crimeLocation = result.filter(x => (x.toString().contains(location))).take(5).foreach(println)

   // val crimeInLocation = crimeLocation.filter(x => (x.toString().contains(crime))).take(5).foreach(println)

  }

  def main(args: Array[String]) = sparkJob("BATTERY", "S YALES")

}