package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import dbis.stark.STObject
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.TestUtils
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.indexed.RTree

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
    TestUtils.createRDD(sc).index(cost = 10, cellSize = 1)
  } 
  
  "An INDEXED SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = createRDD()
    
    val parti = rdd.partitioner.get.asInstanceOf[BSPartitioner[STObject, (STObject, (String, Int, String, STObject))]]
    
//    parti.printHistogram("/home/hage/histo")
//    parti.printPartitions("/home/hage/parts")
    
    val foundPoints = rdd.intersect(qry).flatten.collect()
    
    withClue("wrong number of intersected points") { foundPoints.size shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in { 
    val rdd = createRDD()
    
    val foundPoints = rdd.containedby(qry).flatten.collect()
    
    withClue("wrong number of points contained by query object") { foundPoints.size shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rdd = createRDD()
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q = STObject("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).flatten.collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  ignore should "find the correct nearest neighbors" in { 
    val rdd = createRDD()
	  
	  val q: STObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	      
  } 
  
  it should "compute the correct join result" in {
    
    val rdd1 = createRDD()
    val rdd2 = TestUtils.createRDD(sc)
    
    val res = rdd1.join(rdd2, { case (l,r) => l.getCentroid.intersects(r.getCentroid) })
    
//    res.count() shouldBe rdd2.count() 
    res.count()
  }
  
  it should "have correct types for chained executions" in  {
    val q = STObject("POINT (53.483437 -2.2040706)")
    val rdd1 = createRDD()
    val res = rdd1.contains(q)
    
    res.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true
    
    val res2 = res.intersect(qry)
    res2.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true

    val res3 = res2.join(rdd1.flatten, (g1, g2) => false)
    
    res3.collect().size shouldBe 0
    
  }
  
}