package dbis.stark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Jacob on 21.02.2017.
  */


object TesterUtil {
  def main(args: Array[String]) {
println("asd")
    val arr = "-fs src/test/resources/ -p 38 -ar -sf 0.1 -tp -si 10k_1-10000.csv".split(" ")

   new TesterUtil().mainMethod(arr)
  }
}

class TesterUtil extends TestUtil{

  override def createSparkContext(name: String) = {
    TestUtils.createSparkContext(name)
  }

  override def createIntervalRDD(
                         sc: SparkContext,
                         file: String = "src/test/resources/intervaltest.csv",
                         sep: Char = ';',
                         numParts: Int = 8,
                         distinct: Boolean = false): RDD[(STObject, (String, STObject))] = {

    TestUtils.createIntervalRDD(sc,file,sep,numParts,distinct)
  }

}
