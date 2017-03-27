package dbis.stark.spatial

import com.vividsolutions.jts.io.WKTReader
import dbis.stark.STObject._
import dbis.stark.{Interval, STObject, TestUtils}
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner.SpatialGridPartitioner
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

object SpatialRDDTestCase {
  
  private val qryS = "POLYGON((59.263107 -4.618767 , 56.773145 -11.281927, 51.419398 -10.419636, 49.438952 -3.730346, 51.321523 2.227303 , 57.482247 7.009100, 59.263107 -4.618767))"
  val qry = new WKTReader().read(qryS)
  
}

class SpatialRDDTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {

  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    sc = TestUtils.createSparkContext("spatialrddtestcase")
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  "A PLAIN SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = TestUtils.createRDD(sc)
//    .partitionBy(new BSPartitioner(rdd, 1, 100))
    val foundPoints = rdd.intersects(qry).collect()
    
    withClue("wrong number of intersected points") { foundPoints.length shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in { 
    val rdd = TestUtils.createRDD(sc)
    
    val foundPoints = rdd.containedby(qry).collect()
    
    withClue("wrong number of points contained by query object") { foundPoints.length shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rdd = TestUtils.createRDD(sc)
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: STObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).collect()
	  
	  foundGeoms.length shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }

  it should "find all elements that contain a given point with spatial partitioner" in {
    val rdd1 = TestUtils.createRDD(sc)

    val rdd = rdd1.partitionBy(new SpatialGridPartitioner(rdd1,10))

    // we look for all elements that contain a given point.
    // thus, the result should be all points in the RDD with the same coordinates
    val q: STObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
    val foundGeoms = rdd.contains(q).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }
  
  it should "find the correct nearest neighbors" in { 
    val rdd = TestUtils.createRDD(sc)
	  
	  // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
	  val q: STObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  } 
  
  
  it should "find correct self-join result for points with intersect" in {
    
    val rdd1 = TestUtils.createRDD(sc, distinct = true).cache()

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd1, PredicatesFunctions.intersects _).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd1.map{ case (st, v) => (st.toText, 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    val isLength = spatialJoinResult.length
    val shouldLength = plainJoinResult.length
    withClue(s"comparing result sizes"){isLength shouldBe shouldLength}

    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs plainJoinResult
  }
  
  it should "find correct self-join result for points with contains" in {
    
    val rdd1 = TestUtils.createRDD(sc, distinct = true).cache()

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd1, PredicatesFunctions.contains _).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd1.map{ case (st, v) => (st.toText, 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs plainJoinResult
  }
  
  it should "find correct self-join result for points with withinDistance" in {
    
    val rdd1 = TestUtils.createRDD(sc, distinct = true).cache()

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd1, PredicatesFunctions.withinDistance(0, (g1, g2) => g1.getGeo.distance(g2.getGeo)) _).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd1.map{ case (st, v) => (st.toText(), 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.size shouldBe plainJoinResult.size
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs(plainJoinResult)
  }
  
  it should "return a cluster result with all points" in {
    val rdd = TestUtils.createRDD(sc)
    
    val f = new java.io.File("clusterresult")
    TestUtils.rmrf(f) // delete output directory if existing to avoid write problems 

    
    

    val res = rdd.cluster(
        keyExtractor = { case (_,(id, _, _,_)) => id } , // key extractor to extract point IDs from tuple //keyExtractor = _._2._1,
        minPts = 10, 
        epsilon = 5.0,  
        maxPartitionCost = 500,
        includeNoise = true,
        outfile = Some(f.toString))
    
    res.count() shouldBe rdd.count() 
  } 
  
  
  it should "intersect with temporal instant" in {
    
    val rdd = TestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val qryT = STObject(qry.getGeo, Interval(TestUtils.makeTimeStamp(2013, 1, 1), TestUtils.makeTimeStamp(2013, 1, 31)))
    
    val res = rdd.intersects(qryT)
    
    res.count() shouldBe 1    
  }
  
  it should "contain with temporal instant" in {
    
    val rdd = TestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", TestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.contains(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal instant" in {
    
    val rdd = TestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", TestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.containedby(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal interval" in {
    
    val rdd = TestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", Interval(TestUtils.makeTimeStamp(2013, 6, 1),TestUtils.makeTimeStamp(2013, 6, 30) ))
    
    val res = rdd.containedby(q)
    
    res.count() shouldBe 4
  }
  
}
