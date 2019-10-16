package dbis.stark.spatial.indexed.live

import dbis.stark.STObject._
import dbis.stark._
import dbis.stark.spatial.PredicatesFunctions
import dbis.stark.spatial.indexed.RTreeConfig
import dbis.stark.spatial.partitioner.{BSPStrategy, BSPartitioner, SpatialGridPartitioner}
import org.apache.spark.SparkContext
import org.apache.spark.SpatialRDD._
import org.locationtech.jts.io.WKTReader
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

object SpatialRDDLiveIndexedTestCase {
  
  private val qryS = "POLYGON((59.263107 -4.618767 , 56.773145 -11.281927, 51.419398 -10.419636, 49.438952 -3.730346, 51.321523 2.227303 , 57.482247 7.009100, 59.263107 -4.618767))"
  val qry = new WKTReader().read(qryS)
  
}

class SpatialRDDLiveIndexedTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {

  import SpatialRDDLiveIndexedTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    sc = StarkTestUtils.createSparkContext("spatialrddtestcase")
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  "A LIVE indexed SpatialRDD" should "find the correct intersection result for points with grid partitioning" in { 
    
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(SpatialGridPartitioner(rddRaw, 5, pointsOnly = true),10)
    
    val foundPoints = rdd.intersects(qry).collect()
    
    withClue("wrong number of intersected points") { foundPoints.length shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query with grid partitioning"  in { 
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(SpatialGridPartitioner(rddRaw, 5, pointsOnly = false),10)
    
    val foundPoints = rdd.containedby(qry).collect()
    
    withClue("wrong number of points contained by query object") { foundPoints.length shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(SpatialGridPartitioner(rddRaw, 5, pointsOnly = true),10)
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: STObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).collect()
	  
	  foundGeoms.length shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  it should "find the correct nearest neighbors with grid partitioning" in { 
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(SpatialGridPartitioner(rddRaw, partitionsPerDimension = 5, pointsOnly = true), order= 5)
	  
	  // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
	  val q: STObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()
	  
	  foundGeoms.length shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }

  it should "find the correct nearest neighbors" in {
    val rdd = StarkTestUtils.createRDD(sc).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with aggregate" in {
    val rdd = StarkTestUtils.createRDD(sc).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnAgg(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with take" in {
    val rdd = StarkTestUtils.createRDD(sc).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnTake(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with aggregate with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnAgg(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with take with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).liveIndex(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnTake(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }


  it should "find the correct within distance filter result" in {
    val rdd = StarkTestUtils.createRDD(sc).liveIndex(order = 3)

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.withinDistance(q, ScalarDistance(0), Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}
  }

  it should "find the correct within distance filter result with FixedGrid" in {
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(SpatialGridPartitioner(rddRaw, 10, pointsOnly = true), order = 3)

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.withinDistance(q, ScalarDistance(0), Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}
  }

  it should "find the correct within distance filter result with BSP" in {
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.liveIndex(BSPartitioner(rddRaw, 1, 50, pointsOnly = true), order = 3)

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.withinDistance(q, ScalarDistance(0), Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}
  }

  it should "find correct self-join result for points with intersect with grid partitioning" in {
    
    val rdd = StarkTestUtils.createRDD(sc, distinct = true).cache()
    
    val rdd1 = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true),10)

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd, PredicatesFunctions.intersects, oneToManyPartitioning = false).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd.map{ case (st, _) => (st.toText, 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs plainJoinResult
  }
  
  it should "find correct self-join result for points with contains with grid partitioning" in {
    
    val rdd = StarkTestUtils.createRDD(sc, distinct = true).cache()
    
    val rdd1 = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true),10)

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd, PredicatesFunctions.contains, oneToManyPartitioning = false).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd.map{ case (st, _) => (st.toText, 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs plainJoinResult
  }
  
  it should "find correct self-join result for points with withinDistance with grid partitioning" in {
    
    val rdd = StarkTestUtils.createRDD(sc, distinct = true).cache()
    
    val rdd1 = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true), 10)

    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd, PredicatesFunctions.withinDistance(ScalarDistance(0), Distance.seuclid), oneToManyPartitioning = false).map(_._1._4.toText()).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd2 = rdd.map{ case (st, _) => (st.toText, 1) }
    val plainJoinResult = rdd2.join(rdd2).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult should contain theSameElementsAs plainJoinResult
    
    
  }
  
  ignore should "return a cluster result with all points with grid partitioning" in {
    val rdd1 = StarkTestUtils.createRDD(sc)

    val f = new java.io.File("clusterresult")
    StarkTestUtils.rmrf(f) // delete output directory if existing to avoid write problems

    
    

    val res = rdd1.cluster(
        keyExtractor = { case (_,(id, _, _,_)) => id } , // key extractor to extract point IDs from tuple //keyExtractor = _._2._1,
        minPts = 10, 
        epsilon = 5.0,  
        maxPartitionCost = 500, 
        includeNoise = true,
        outfile = Some(f.toString))
    
    res.count() shouldBe rdd1.count() 
  }
  
  it should "intersect with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val qryT = STObject(qry.getGeo, Interval(StarkTestUtils.makeTimeStamp(2013, 1, 1), StarkTestUtils.makeTimeStamp(2013, 1, 31)))
    
    val res = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true), 10).intersects(qryT)
    
    res.count() shouldBe 1    
  }
  
  it should "contain with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", StarkTestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true), 10).contains(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", StarkTestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true), 10).containedby(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal interval" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", Interval(StarkTestUtils.makeTimeStamp(2013, 6, 1),StarkTestUtils.makeTimeStamp(2013, 6, 30) ))
    
    val res = rdd.liveIndex(SpatialGridPartitioner(rdd, 5, pointsOnly = true), 10).containedby(q)
    
    res.count() shouldBe 4
  }



}