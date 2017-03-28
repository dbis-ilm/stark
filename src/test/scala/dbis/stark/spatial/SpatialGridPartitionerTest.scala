package dbis.stark.spatial

import dbis.stark.TemporalExpressionMatchers._
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import dbis.stark.{Instant, Interval, STObject, TestUtils}

import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial._
import dbis.stark.spatial.indexed.live.{LiveIndexedSpatialRDDFunctions, LiveIntervalIndexedSpatialRDDFunctions}
import org.apache.spark.rdd.RDD

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


  val searchsize = 30;
  val intervalfakt = 1000;
  val searchPolygon: STObject = STObject(s"Polygon((-$searchsize $searchsize, $searchsize $searchsize, $searchsize -$searchsize, -$searchsize -$searchsize, -$searchsize $searchsize))", Interval(2 * intervalfakt, 5 * intervalfakt))


  it should "find the same results"  in {
    val rddRaw = TestUtils.createIntervalRDD(sc,"src/test/resources/10k_1-10000.csv")

    var indexData: SpatialRDDFunctions[STObject, (String, STObject)] = rddRaw
    var res1 = indexData.containedby2(searchPolygon).collect()



    val rdd2 = rddRaw.partitionBy(new SpatialGridPartitioner(rddRaw, 10))
    var indexData2: SpatialRDDFunctions[STObject, (String, STObject)] = rdd2
    val res2 = indexData2.containedby2(searchPolygon).collect()

    println(res1.size)
    println(res2.size)
    res1.size shouldBe res2.size
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