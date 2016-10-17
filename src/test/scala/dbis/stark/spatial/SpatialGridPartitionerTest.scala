package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import dbis.stark.STObject

class SpatialGridPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors() - 1}]").setAppName(getClass.getName)
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  it should "find use cells as partitions for taxi"  in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
//      .filter { arr => !arr(1).contains("0 0")}
      .map { arr => (STObject(arr(1)), arr(0))}
      
      val minMax = SpatialPartitioner.getMinMax(rdd)
      
      val parti = new SpatialGridPartitioner(rdd, partitionsPerDimension = 3, false, minMax, dimensions = 2)
      
      
      
      parti.numPartitions shouldBe 9
      
      rdd.collect().foreach { case (st, name) =>
        try {
          val pNum = parti.getPartition(st)
          withClue(name) { pNum should (be >= 0 and be < 9)}
        } catch {
        case e:IllegalStateException =>
          
          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4
          
          
          
          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
        }
        
      }
      
      
  }
  
  
}