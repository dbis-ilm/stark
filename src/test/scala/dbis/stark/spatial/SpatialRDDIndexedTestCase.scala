package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import dbis.stark.SpatialObject
import dbis.stark.spatial.SpatialRDD._
import dbis.spatial.NPoint

class SpatialRDDIndexedTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("indexedspatialrddtestcase")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  //##############################################################################
  //
  //  NOTE:
  //   The difference to the SpatialRDDTestCase (with plain data)
  //   is that here, in createRDD, the RDD is indexed! 
  //   The actual test cases are the same!
  //
  //##############################################################################
  
  
  def createRDD(file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',') = {
    Helper.createRDD(sc).index(cost = 10, cellSize = 1)
  } 
  
  "An INDEXED SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = createRDD()
    
    val parti = rdd.partitioner.get.asInstanceOf[BSPartitioner[SpatialObject, (SpatialObject, (String, Int, String, SpatialObject))]]
    
//    parti.printHistogram("/home/hage/histo")
//    parti.printPartitions("/home/hage/parts")
    
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
	  val q = SpatialObject("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  ignore should "find the correct nearest neighbors" in { 
    val rdd = createRDD()
	  
	  val q: SpatialObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	      
  } 
}