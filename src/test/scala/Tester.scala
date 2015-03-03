import bigdata.TakeDataInRowsByBatch
import org.scalatest.FunSuite
import scala.collection.mutable.Stack

/**
 * Created by Kehinde on 15-03-01.
 */
class Tester extends FunSuite{

  test("spark job returns data") {
    assert(TakeDataInRowsByBatch.sparkJob()===null)
  }


}
