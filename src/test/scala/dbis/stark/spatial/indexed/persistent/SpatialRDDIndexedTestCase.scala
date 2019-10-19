package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject.{fromWKT, getInternal, makeSTObject}
import dbis.stark._
import dbis.stark.spatial.indexed.{Index, RTreeConfig}
import dbis.stark.spatial.partitioner.{BSPStrategy, BSPartitioner, GridStrategy, SpatialGridPartitioner}
import dbis.stark.spatial.{PredicatesFunctions, SpatialRDDTestCase}
import org.apache.spark.SpatialRDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.index.strtree.RTree
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SpatialRDDIndexedTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {
  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  private var indexedRDD: RDD[Index[(STObject, (String,Long, String, STObject))]] = _
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("indexedspatialrddtestcase").set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
    sc = new SparkContext(conf)
//    sc = new STSparkContext(conf)

    indexedRDD = StarkTestUtils.timing("partition (cellsize=10) + index (order=5)") {
      StarkTestUtils.createIndexedRDD(sc, cost = 100, cellSize = 10, order = 5).cache()
    }
    println(indexedRDD.count())
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
  
  
//  "An INDEXED SpatialRDD" should "find the correct intersection result for points (RO)" in {
//
//    val rdd = TestUtils.createIndexedRDD(sc, cost = 100, cellSize = 10, order = 5)
//
////    val parti = rdd.partitioner.get.asInstanceOf[BSPartitioner[STObject, (STObject, (String, Int, String, STObject))]]
//
////    parti.printHistogram("/home/hage/histo")
////    parti.printPartitions("/home/hage/parts")
//
//    val start = System.currentTimeMillis()
//    val foundPoints = rdd.intersectsRO(qry).flatten.collect()
//    val end = System.currentTimeMillis()
//    println(s"intersect + flatten: ${end - start} ms")
//
//    withClue("wrong number of intersected points") { foundPoints.size shouldBe 36 } // manually counted
//
//    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
//
//  }
//
//  it should "find all elements contained by a query (RO)" in {
//    val rdd = TestUtils.createIndexedRDD(sc, cost = 100, cellSize = 10, order = 5)
//
//    val start = System.currentTimeMillis()
//    val foundPoints = rdd.containedbyRO(qry).flatten.collect()
//    val end = System.currentTimeMillis()
//    println(s"contaiedby + flatten: ${end - start} ms")
//
//    withClue("wrong number of points contained by query object") { foundPoints.size shouldBe 36 } // manually counted
//  }
//
//  it should "find all elements that contain a given point (RO)" in {
//	  val rdd = TestUtils.createIndexedRDD(sc, cost = 100, cellSize = 10, order = 5)
//
//	  // we look for all elements that contain a given point.
//	  // thus, the result should be all points in the RDD with the same coordinates
//	  val q = STObject("POINT (53.483437 -2.2040706)")
//	  val foundGeoms = rdd.containsRO(q).flatten.collect()
//
//	  foundGeoms.size shouldBe 6
//	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
//
//  }

  "An INDEXED SpatialRDD" should "find the correct intersection result for points" in {



    val foundPoints = StarkTestUtils.timing("intersects + collect") {
      indexedRDD.intersects(qry).collect()
    }

    withClue("wrong number of intersected points") { foundPoints.length shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in {
//    val rdd = StarkTestUtils.timing("partition (cellsize=0.10) + index (order=5)") {
//      StarkTestUtils.createIndexedRDD(sc, cost = 100, cellSize = 0.10, order = 5)
//    }

    val foundPoints = StarkTestUtils.timing("containedby + collect") {
      indexedRDD.containedby(qry).collect()
    }

    withClue("wrong number of points contained by query object") { foundPoints.length shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 

	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q = STObject("POINT (53.483437 -2.2040706)")
	  val foundGeoms = indexedRDD.contains(q).collect()
	  
	  foundGeoms.length shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  it should "find the correct nearest neighbors with Grid Partitioning" in { 
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.index(SpatialGridPartitioner(rddRaw, partitionsPerDimension = 5, pointsOnly = false), order= 5)
	  
	  // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
	  val q: STObject = "POINT (53.483437 -2.2040706)"

    StarkTestUtils.timing("normal with grid") {
      val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()

      foundGeoms.length shouldBe 6
      foundGeoms.foreach { case (g, _) => g shouldBe q }
    }
  }

  it should "be faster with kNN2 vs kNN" in {
    var rdd = StarkTestUtils.createRDD(sc)
    var i = 0
    while(i < 100) {
      rdd = rdd.union(StarkTestUtils.createRDD(sc))
      i += 1
    }

    val k = 50

    val indexConf  = RTreeConfig(order = 10)
//    val partConf = RTreeStrategy(order = 10, pointsOnly = true, minmax = None, sampleFraction = 0)
    val partConf = GridStrategy(partitionsPerDimensions = 10, pointsOnly = true, minmax = None, sampleFraction = 0)

    val theRDD = rdd.map{case (so, _) => (so,1)}.index(partConf, indexConf).cache()

    println(theRDD.count())
//    val q: STObject = "POINT (53.483437 -2.2040706)"
        val q: STObject = "POINT (-2.2040706 53.483437 )"


    var t0 = System.currentTimeMillis()
    val knn = theRDD.knnAggIter(q, k, Distance.seuclid).size
    var t1 = System.currentTimeMillis()
    println(s"knn:\t${t1 - t0}")


    t0 = System.currentTimeMillis()
    val knn2 = theRDD.knn2(q, k, Distance.seuclid).size
    t1 = System.currentTimeMillis()
    println(s"knn2:\t${t1 - t0}")

//    withClue("knn vs agg"){knn should contain theSameElementsAs knn2}

    knn2 shouldBe knn

//    knn2.map(_.toString()).sorted.mkString("\n") shouldBe knn.map(_.toString()).sorted.mkString("\n")
  }

  it should "find the correct nearest neighbors" in {
    val rdd = StarkTestUtils.createRDD(sc).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with aggregate" in {
    val rdd = StarkTestUtils.createRDD(sc).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnAgg(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with take" in {
    val rdd = StarkTestUtils.createRDD(sc).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnTake(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.kNN(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with aggregate with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnAgg(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }

  it should "find the correct nearest neighbors with take with BSP" in {
    val rdd = StarkTestUtils.createRDD(sc).partitionBy(BSPStrategy(cellSize = 1,maxCost = 100,pointsOnly = true)).index(RTreeConfig(order = 5))

    // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
    val q: STObject = "POINT (53.483437 -2.2040706)"
    val foundGeoms = rdd.knnTake(q, 6, Distance.seuclid).collect()

    foundGeoms.length shouldBe 6
    foundGeoms.foreach{ case (g,_) => g shouldBe q}

  }




  it should "find the correct nearest neighbors with BSP (compare ith tree)" in {
    val rddRaw = StarkTestUtils.createRDD(sc)
    val rdd = rddRaw.index(BSPartitioner(rddRaw,  1, 100, pointsOnly = true), order= 5) // 0.5
	  
    val k = 6
    
	  // we know that there are 5 duplicates in the data for this point.
    // Hence, the result should contain the point itself and the 5 duplicates
	  val q: STObject = "POINT (53.483437 -2.2040706)"

    StarkTestUtils.timing("normal with bsp") {
      val foundGeoms = rdd.kNN(q, k, Distance.seuclid).collect()
      foundGeoms.length shouldBe k
      foundGeoms.foreach{ case (g,_) => withClue("found geoms did not match") {g shouldBe q}}
    }

    val tree = new RTree[(String, Long, String, STObject)](5)
    rddRaw.collect().foreach{ case (so, v) => tree.insert(so, v) }
    tree.build()

    val refGeoms = tree.kNN(q, k, Distance.seuclid).toList
    refGeoms.size shouldBe k
    refGeoms.foreach{ case ((_,_,_,g),_) => withClue("reference geoms did not match") {g shouldBe q }}
  }
  
  it should "compute the correct (quasi) self-join result for points with intersects" in {
    val rdd1 = StarkTestUtils.createIndexedRDD(sc, distinct = true, cost = 100, cellSize = 10, order = 5)
    val rdd2 = StarkTestUtils.createRDD(sc, distinct = true)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, PredicatesFunctions.intersects _, oneToMany = false).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = StarkTestUtils.createRDD(sc, distinct = true).map{ case (st, _) => (st.toText, 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs plainJoinResult
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with contains" in {
    val rdd1 = StarkTestUtils.createIndexedRDD(sc, distinct = true, cost = 100, cellSize = 10, order = 5)
    val rdd2 = StarkTestUtils.createRDD(sc, distinct = true)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, PredicatesFunctions.contains _, oneToMany = false).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = StarkTestUtils.createRDD(sc, distinct = true).map{ case (st, _) => (st.toText, 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs plainJoinResult
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with containedBy" in {
    
    val rdd1 = StarkTestUtils.createIndexedRDD(sc, distinct = true, cost = 100, cellSize = 10, order = 5)
    val rdd2 = StarkTestUtils.createRDD(sc, distinct = true)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, PredicatesFunctions.containedby _, oneToMany = false).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = StarkTestUtils.createRDD(sc, distinct = true).map{ case (st, _) => (st.toText, 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs plainJoinResult
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }
  
  it should "compute the correct (quasi) self-join result for points with withinDistance" in {
    
    val rdd1 = StarkTestUtils.createIndexedRDD(sc, distinct = true, cost = 100, cellSize = 10, order = 5)
    val rdd2 = StarkTestUtils.createRDD(sc, distinct = true)
    
    /* perform the spatial join with intersects predicate
     * and then map the result to the STObject element (which is the same for left and right input)
     * This is done for comparison later
     */
    val spatialJoinResult = rdd1.join(rdd2, PredicatesFunctions.withinDistance(ScalarDistance(0), Distance.seuclid) _, oneToMany = false).collect()

    /* We compare the spatial join result to a normal join performed by traditional Spark
     * an the String representation of the STObject. Since we need a pair RDD, we use the
     * STObject's text/string representation as join key and a simple 1 as payload.
     * We also map the result to just the text of the respective STObject. 
     */
    val rdd3 = StarkTestUtils.createRDD(sc, distinct = true).map{ case (st, _) => (st.toText, 1) }
    
    val plainJoinResult = rdd3.join(rdd3).map(_._1).collect() // plain join
    
    // first of all, both sizes should be the same
    spatialJoinResult.length shouldBe plainJoinResult.length
    
    // and they both should contain the same elements (we don't care abour ordering)
    spatialJoinResult.map(_._1._4.toText()) should contain theSameElementsAs plainJoinResult
    
    spatialJoinResult.foreach{ case ((_, _, _, lLoc), (_, _, _, rLoc)) => 
      lLoc shouldBe rLoc // we joined on points with "intersects", hence they should be equal
    }
  }

//  it should "have correct types for chained executions" in  {
//    val q = STObject("POINT (53.483437 -2.2040706)")
//    val rdd1 = TestUtils.createIndexedRDD(sc, cost = 100, cellSize = 10, order = 5)
//    val res = rdd1.contains(q)
//
//    res.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true
//
//    val res2 = res.intersects(qry)
//    res2.isInstanceOf[RDD[RTree[STObject, (STObject, (String, Int, String, STObject))]]] shouldBe true
//
//    val res3 = res2.join(rdd1.flatten, (g1, g2) => false)
//
//    res3.collect().size shouldBe 0
//
//  }
  
  it should "intersect with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val qryT = STObject(qry.getGeo, Interval(StarkTestUtils.makeTimeStamp(2013, 1, 1), StarkTestUtils.makeTimeStamp(2013, 1, 31)))
    
    val res = rdd.index(SpatialGridPartitioner(rdd, 5, pointsOnly = false), 10).intersects(qryT)
    
    res.count() shouldBe 1    
  }
  
  it should "contain with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", StarkTestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.index(SpatialGridPartitioner(rdd, 5, pointsOnly = false), 10).contains(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal instant" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", StarkTestUtils.makeTimeStamp(2013, 6, 8))
    
    val res = rdd.index(SpatialGridPartitioner(rdd, 5, pointsOnly = false), 10).containedby(q)
    
    res.count() shouldBe 2
  }
  
  it should "containedby with temporal interval" in {
    
    val rdd = StarkTestUtils.createRDD(sc).map{ case (so, (id, ts, desc, _)) => (STObject(so.getGeo, ts), (id, desc)) }
    
    val q: STObject = STObject("POINT (53.483437 -2.2040706)", Interval(StarkTestUtils.makeTimeStamp(2013, 6, 1),StarkTestUtils.makeTimeStamp(2013, 6, 30) ))
    
    val res = rdd.index(SpatialGridPartitioner(rdd, 5, pointsOnly = false), 10).containedby(q)
    
    res.count() shouldBe 4
  }
  
}
