package dbis.stark

import com.vividsolutions.jts.index.intervalrtree.SortedPackedIntervalRTree
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.{Dataset, SparkSession, SQLContext}

/**
  * Created by Jacob on 21.02.2017.
  */


object TesterUtil {
  def main(args: Array[String]) {
    //println("asd")
    var arr = args

    // damit ich nicht ständig die run configurations ändern muss
    arr = "-fs src/test/resources/ -ds -p 38 -ar -sf 0.1 -np -ni 10M_1-10000.csv".split(" ")

   new TesterUtil().mainMethod(arr)
  }
}

class TesterUtil extends TestUtil{

  override def createSparkContext(name: String) = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(name)
    new SparkContext(conf)
  }

  override def createSparkSession(name: String) = {
    SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]")
      .getOrCreate()
  }



}
case class STO(id: Long, stob: String, start : Long, end: Long)