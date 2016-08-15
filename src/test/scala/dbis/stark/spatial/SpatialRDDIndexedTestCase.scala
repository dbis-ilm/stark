package dbis.stark.spatial

import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.apache.spark.SparkConf
import dbis.stark.SpatialObject
import dbis.stark.spatial.SpatialRDD._

class SpatialRDDIndexedTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("spatialrddtestcase")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  def createRDD(file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',') = {
    sc.textFile(file, 2)
      .map { line => line.split(sep) }
      .map { arr => (arr(0), arr(1).toInt, arr(2), SpatialObject(arr(7))) }
      .keyBy( _._4)
      .index(cost = 10, cellSize = 0.1)
  } 
  
  "An INDEXED SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = createRDD()
    
    val foundPoints = rdd.intersect(qry).collect()
    
    withClue("wrong number of intersected points") { foundPoints.size shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in { 
    val rdd = createRDD()
    
    val foundPoints = rdd.containedby(qry).collect()
    
    withClue("wrong number of points contained by query object") { foundPoints.size shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rdd = createRDD()
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: SpatialObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  it should "find the correct nearest neighbors" in { 
    val rdd = createRDD()
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: SpatialObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  } 
}