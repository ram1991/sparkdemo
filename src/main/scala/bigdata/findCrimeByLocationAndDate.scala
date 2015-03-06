package bigdata

import java.sql.Timestamp

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormat

/**
 * Created by Kehinde on 15-03-05.
 */
object findCrimeByLocationAndDate {  // Specify the path to your data file
val conf = new SparkConf().setAppName("Spark BigData").setMaster("local")

  val sc = new SparkContext(conf)

  def sparkJob(crime: String, location: String, startDate:String, endDate:String) = {

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

    val duration = computeTimeDifference (startDate, endDate)

    val result = dataRDD.map(x => (x("ID"), x("Date"), x("Block"), x("Description"), x("Location Description"), computeTimeDifference(x("Date"),endDate))).take(20)


    val crimeLocationBetweenDate = result.filter(x => ((x._6 >= 0) && (x._6 <= duration) && (x.toString().contains(crime)))).foreach(println)

   // val crimeInLocation = crimeLocation.filter(x => (x.toString().contains(crime))).take(5).foreach(println)



  }

  def computeTimeDifference (firstDate:String, secondDate:String)= {


    val dtForm=DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a")

    val mFirstDate=dtForm.parseDateTime(firstDate);
    val mSecondDate=dtForm.parseDateTime(secondDate);
    val duration= new Duration(mFirstDate,mSecondDate)

    duration.getStandardDays

  }

  def main(args: Array[String]) = sparkJob("BATTERY", "SOUTH CHICAGO", "1/01/2015 04:50:00 PM", "12/12/2015 04:50:00 PM")

}