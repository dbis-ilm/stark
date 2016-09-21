package dbis.stark.spatial.indexed.persistent

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
import dbis.stark.STObject.getInternal
import dbis.stark.STObject.makeSTObject
import dbis.stark.STObject.stringToGeom
import dbis.stark.spatial.BSPartitioner
import dbis.stark.spatial.Predicates
import dbis.stark.spatial.SpatialRDDTestCase
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class SpatialRDDIndexedTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("indexedspatialrddtestcase").set("spark.ui.showConsoleProgress", "false")
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
  //   The actual test cases should be the same!
  //
  //##############################################################################
  
  
  "An INDEXED SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = TestUtils.createIndexedRDD(sc)
    
//    val parti = rdd.partitioner.get.asInstanceOf[BSPartitioner[STObject, (STObject, (String, Int, String, STObject))]]
    
//    parti.printHistogram("/home/hage/histo")
//    parti.printPartitions("/home/hage/parts")
    
    val start = System.currentTimeMillis()
    val foundPoints = rdd.intersects(qry).flatten.collect()
    val end = System.currentTimeMillis()
    println(s"intersect + flatten: ${end - start} ms")
    
    withClue("wrong number of intersected points") { foundPoints.size shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in { 
    val rdd = TestUtils.createIndexedRDD(sc)
    
    val start = System.currentTimeMillis()
    val foundPoints = rdd.containedby(qry).flatten.collect()
    val end = System.currentTimeMillis()
    println(s"contaiedby + flatten: ${end - start} ms")
    
    withClue("wrong number of points contained by query object") { foundPoints.size shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rdd = TestUtils.createIndexedRDD(sc)
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q = STObject("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).flatten.collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  ignore should "find the correct nearest neighbors" in { 
    val rdd = TestUtils.createIndexedRDD(sc)
	  
	  val q: STObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	      
  } 
  
  it should "compute the correct (quasi) self-join result for points with intersects" in {
    
    val rdd1 = TestUtils.createIndexedRDD(sc, distinct = true, numParts = 4, cost = 100)
    val rdd2 = TestUtils.createRDD(sc, distinct = true, numParts = 4)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, Predicates.intersects _).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = TestUtils.createRDD(sc, distinct = true, numParts = 4).map{ case (st, v) => (st.toText(), 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.size shouldBe plainJoinResult.size
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs(plainJoinResult)
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with contains" in {
    
    val rdd1 = TestUtils.createIndexedRDD(sc, distinct = true, numParts = 4, cost = 100)
    val rdd2 = TestUtils.createRDD(sc, distinct = true, numParts = 4)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, Predicates.contains _).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = TestUtils.createRDD(sc, distinct = true, numParts = 4).map{ case (st, v) => (st.toText(), 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.size shouldBe plainJoinResult.size
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs(plainJoinResult)
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with containedBy" in {
    
    val rdd1 = TestUtils.createIndexedRDD(sc, distinct = true, numParts = 4, cost = 100)
    val rdd2 = TestUtils.createRDD(sc, distinct = true, numParts = 4)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, Predicates.containedby _).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = TestUtils.createRDD(sc, distinct = true, numParts = 4).map{ case (st, v) => (st.toText(), 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.size shouldBe plainJoinResult.size
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs(plainJoinResult)
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with withinDistance" in {
    
    val rdd1 = TestUtils.createIndexedRDD(sc, distinct = true, numParts = 4, cost = 100)
    val rdd2 = TestUtils.createRDD(sc, distinct = true, numParts = 4)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, Predicates.withinDistance(0, (s1,s2) => s1.getGeo.distance(s2.getGeo)) _).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = TestUtils.createRDD(sc, distinct = true, numParts = 4).map{ case (st, v) => (st.toText(), 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.size shouldBe plainJoinResult.size
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs(plainJoinResult)
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "have correct types for chained executions" in  {
    val q = STObject("POINT (53.483437 -2.2040706)")
    val rdd1 = TestUtils.createIndexedRDD(sc)
    val res = rdd1.contains(q)
    
    res.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true
    
    val res2 = res.intersects(qry)
    res2.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true

    val res3 = res2.join(rdd1.flatten, (g1, g2) => false)
    
    res3.collect().size shouldBe 0
    
  }
  
}
