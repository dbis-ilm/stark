package dbis.stark.spatial.partitioner

import dbis.stark.spatial._
import dbis.stark.spatial.indexed.RTreeConfig
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.{Fix, STObject, StarkKryoRegistrator, StarkTestUtils}
import org.apache.spark.SpatialRDD._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext, SpatialRDD}
import org.locationtech.jts.io.WKTReader
import org.scalatest.tagobjects.Slow
import org.scalatest._

object Sampling extends Tag("dbis.stark.Sampling")

trait TestTimer extends FlatSpec with BeforeAndAfter {
  var start: Long = 0
  before {
    start = System.currentTimeMillis()
  }

  after {
    if(System.getProperty("testtiming") != null) {
      val dur = System.currentTimeMillis() - start

      println(s"\t -> $dur ms")
    }
  }
}

class BSPartitionerTest extends TestTimer with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]").setAppName("paritioner_test2")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }


  private def createRDD(points: Seq[String] = List("POINT(2 2)", "POINT(2.55 2.5)", "POINT(2 4)", "POINT(4 2)", "POINT(4 4)")): RDD[(STObject, Long)] =
    sc.parallelize(points,4).zipWithIndex()
      .map { case (string, id) => (new WKTReader().read(string), id) }

  //  "The BSP partitioner"

  it  should "find correct min/max values" in {

    val rdd = createRDD()
    val sideLength = 1
    val parti = BSPartitioner(rdd, sideLength, 1, pointsOnly = true)

    withClue("wrong minX value") { parti.minX shouldBe 2 }
    withClue("wrong minX value") { parti.minY shouldBe 2 }
    withClue("wrong minX value") { parti.maxX shouldBe 4 + sideLength } // max values are set to +1 to have "right open" intervals
    withClue("wrong minX value") { parti.maxY shouldBe 4 + sideLength } // max values are set to +1 to have "right open" intervals
  }

  it  should "have the correct min/max in real world scenario" in {

    val rdd = StarkTestUtils.createRDD(sc)

    val parti = BSPartitioner(rdd,1, 10, pointsOnly = true)

    parti.minX shouldBe -35.8655
    parti.maxX shouldBe 62.1345
    parti.minY shouldBe -157.74538
    parti.maxY shouldBe 153.25462

  }

  //  it  should "have the correct number of x cells in reald world scenario with length = 1" in {
  //
  //    val rdd = StarkTestUtils.createRDD(sc)
  //
  //    val parti = BSPartitioner(rdd,1, 10, pointsOnly = true)
  //
  //    parti.numXCells shouldBe 98 // ?
  //
  //  }
  //
  //
  //  it  should "find the correct number of cells for X dimension" in {
  //    val rdd = createRDD()
  //
  //    val parti = BSPartitioner(rdd, 1, 1, pointsOnly = true)
  //
  //    parti.numXCells shouldBe 3
  //  }

  //  it  should "create correct number of cells" in {
  //
  //    val rdd = createRDD()
  //
  //    val parti = BSPartitioner(rdd, 1, 1, pointsOnly = true)
  //
  ////    println(s"${parti.cells.mkString("\n")}")
  //
  //    val rddSize = rdd.count()
  //
  //    withClue("number of points does not match"){parti.histogram.map(_._2).sum shouldBe rddSize}
  //
  //    parti.histogram.length shouldBe 9
  //
  //
  //  }

  //  it  should "create correct cell histogram" in {
  //
  //    val rdd = createRDD()
  //
  //    val parti = BSPartitioner(rdd, 1, 1, pointsOnly = true)
  //
  //    val shouldSizes = Array(
  //      (Cell(NRectRange(NPoint(2,2), NPoint(3,3)),NRectRange(NPoint(2,2), NPoint(3,3))), 2), // 0
  //      (Cell(NRectRange(NPoint(3,2), NPoint(4,3)),NRectRange(NPoint(3,2), NPoint(4,3))), 0), // 1
  //      (Cell(NRectRange(NPoint(4,2), NPoint(5,3)),NRectRange(NPoint(4,2), NPoint(5,3))), 1), // 2
  //      (Cell(NRectRange(NPoint(2,3), NPoint(3,4)),NRectRange(NPoint(2,3), NPoint(3,4))), 0), // 3
  //      (Cell(NRectRange(NPoint(3,3), NPoint(4,4)),NRectRange(NPoint(3,3), NPoint(4,4))), 0), // 4
  //      (Cell(NRectRange(NPoint(4,3), NPoint(5,4)),NRectRange(NPoint(4,3), NPoint(5,4))), 0), // 5
  //      (Cell(NRectRange(NPoint(2,4), NPoint(3,5)),NRectRange(NPoint(2,4), NPoint(3,5))), 1), // 6
  //      (Cell(NRectRange(NPoint(3,4), NPoint(4,5)),NRectRange(NPoint(3,4), NPoint(4,5))), 0), // 7
  //      (Cell(NRectRange(NPoint(4,4), NPoint(5,5)),NRectRange(NPoint(4,4), NPoint(5,5))), 1)  // 8
  //    )
  //
  //    parti.histogram.buckets.values should contain only (shouldSizes:_*)
  //  }

  //  it should "have correct grid" in {
  //    val rdd = createRDD()
  //
  //    val parti = BSPartitioner(rdd, 1, 1, pointsOnly = true)
  //
  //    val shouldSizes = Array(
  //      (Cell(0,NRectRange(NPoint(2,2), NPoint(3,3)),NRectRange(NPoint(2,2), NPoint(3,3))),2),
  //      (Cell(1,NRectRange(NPoint(3,2), NPoint(4,3)),NRectRange(NPoint(3,2), NPoint(4,3))),0),
  //      (Cell(2,NRectRange(NPoint(4,2), NPoint(5,3)),NRectRange(NPoint(4,2), NPoint(5,3))),1),
  //      (Cell(3,NRectRange(NPoint(2,3), NPoint(3,4)),NRectRange(NPoint(2,3), NPoint(3,4))),0),
  //      (Cell(4,NRectRange(NPoint(3,3), NPoint(4,4)),NRectRange(NPoint(3,3), NPoint(4,4))),0),
  //      (Cell(5,NRectRange(NPoint(4,3), NPoint(5,4)),NRectRange(NPoint(4,3), NPoint(5,4))),0),
  //      (Cell(6,NRectRange(NPoint(2,4), NPoint(3,5)),NRectRange(NPoint(2,4), NPoint(3,5))),1),
  //      (Cell(7,NRectRange(NPoint(3,4), NPoint(4,5)),NRectRange(NPoint(3,4), NPoint(4,5))),0),
  //      (Cell(8,NRectRange(NPoint(4,4), NPoint(5,5)),NRectRange(NPoint(4,4), NPoint(5,5))),1)
  //    )
  //
  ////    parti.cells.buckets.foreach(c => println(c._2._1.range.wkt))
  ////    println("---")
  ////    shouldSizes.foreach(c => println(c.range.wkt))
  //    parti.histogram.length shouldBe shouldSizes.length
  //
  //    shouldSizes.foreach{ shouldCell =>
  //      withClue(s"cell ${shouldCell._1.id} did not match") { parti.histogram(shouldCell._1.id)._1 shouldBe shouldCell._1 }
  //      withClue(s"count ${shouldCell._1.id} did not match") { parti.histogram(shouldCell._1.id)._2 shouldBe shouldCell._2 }
  //
  //    }
  //  }


  it  should "return the correct partition id" taggedAs Fix in {
    val rdd = createRDD()
    val parti = BSPartitioner(rdd, 0.1, 1, pointsOnly = true)
    parti.printPartitions("/tmp/idtest_partitions")
//
//    val parts = rdd.map{ case (g,_) =>
//      val pid = parti.getPartition(g)
//      println(s"${g.getGeo} --> $pid")
//        pid
//    }.collect().toSet

//    parts.size shouldBe 5

    rdd.collect().foreach{ case (so,_) =>
      withClue(s"$so"){parti.partitions.map(_.range).exists{r =>
        val c = so.getGeo.getCentroid
        val p = NPoint(c.getX, c.getY)
        r.contains(p)
      } shouldBe true
      }
    }
  }


  it  should "return all points for one partition" in {

    val rdd: RDD[(STObject, (String, Long, String, STObject))] = StarkTestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())

    // with maxcost = size of RDD everything will end up in one partition
    val parti = BSPartitioner(rdd, 2, maxCostPerPartition = 1000, pointsOnly = false)

    val shuff = new ShuffledRDD(rdd, parti)

    shuff.count() shouldBe rdd.count()

  }

  it  should "return all points for two partitions" in {

    val rdd: RDD[(STObject, (String, Long, String, STObject))] = StarkTestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())

    // with maxcost = size of RDD everything will end up in one partition
    val parti = BSPartitioner(rdd, 2, maxCostPerPartition = 500, pointsOnly = true)

    val shuff = new ShuffledRDD(rdd, parti)
    // insert dummy action to make sure Shuffled RDD is evaluated
    shuff.foreach{_ => }

    shuff.count() shouldBe 1000
    shuff.count() shouldBe rdd.count()

  }

  it  should "return all points for max cost 100 & sidelength = 1" in {

    val rdd: RDD[(STObject, (String, Long, String, STObject))] = StarkTestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())

    // with maxcost = size of RDD everything will end up in one partition
    val parti = BSPartitioner(rdd, 1, maxCostPerPartition = 100, pointsOnly = true)

    val shuff = new ShuffledRDD(rdd, parti)
    // insert dummy action to make sure Shuffled RDD is evaluated
    shuff.foreach{_ => }

    shuff.count() shouldBe 1000
    shuff.count() shouldBe rdd.count()

  }

  it  should "return only one partition if max cost equals input size" in {

    val rdd: RDD[(STObject, (String, Long, String, STObject))] = StarkTestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())

    // with maxcost = size of RDD everything will end up in one partition
    val parti = BSPartitioner(rdd, 1, maxCostPerPartition = 1000, pointsOnly = true)

    parti.numPartitions shouldBe 1

  }

  /* this test case was created for bug hunting, where for the taxi data, all points with coordinates (0 0) were
   * not added to any partition.
   *
   * The result was that the cell bounds for the histogram where created wrong.
   */
  it  should "work with 0 0 " in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", Runtime.getRuntime.availableProcessors())
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val rddSize = rdd.count()

    val minMax = GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal))

    BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
    val parti = BSPartitioner(rdd, 0.1, 10*1000, pointsOnly = true, minMax) // disable sampling

    parti.printPartitions("/tmp/partitions00.wkt")
    //    parti.printHistogram(java.nio.file.Paths.get("/tmp/histo00.wkt"))

    // make sure there are no duplicate cells, i.e. they shouldn't have the same region
    //    withClue("no duplicate cells"){
    //      parti.histogram.buckets.values.map { case (cell, _) => cell.range }.toList.distinct.length shouldBe parti.histogram.length
    //    }

    //    withClue("histogram counts"){parti.histogram.map(_._2).sum shouldBe rddSize}

    var oob = 0
    // every point must be in one partition
    rdd.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < parti.numPartitions) }
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          //          println(s"idx out of bounds for $st ($name)")
          oob += 1
        case e:IllegalStateException =>

          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4

          parti.partitions.foreach { cell =>

            val xOk = st.getGeo.getCentroid.getX >= cell.range.ll(0) && st.getGeo.getCentroid.getX <= cell.range.ur(0)
            val yOk = st.getGeo.getCentroid.getY >= cell.range.ur(1) && st.getGeo.getCentroid.getY <= cell.range.ur(1)

            println(s"${cell.id} cell: ${cell.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))}  x: $xOk  y: $yOk")
          }
          //        val containingCell = parti.histogram.buckets.values.find(cell => cell._1.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY)))
          //        if(containingCell.isDefined) {
          //          println(s"should be in ${containingCell.get._1.id} which has bounds ${parti.histogram(containingCell.get._1.id)._1.range} and count ${parti.histogram(containingCell.get._1.id)._2}")
          //        } else {
          //          println("No cell contains this point!")
          //        }
          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
      }
    }

    if(oob != 0)
      fail(s"out of bounds = $oob  (rdd size= $rddSize")
  }

  it should "use cells as partitions for taxi" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMax = GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal))

    //    BSPartitioner.numCellThreshold = -5
    val parti = BSPartitioner(rdd, 1, 100, pointsOnly = true, minMax) // disable sampling

    //    val nonempty = parti.histogram.buckets.values.filter(_._2 > 0)
    //    withClue("number of non empty cells") { nonempty.length shouldBe 7 }

    //    parti.histogram.nonEmptyCells should contain theSameElementsAs nonempty

    //    val cnt = rdd.count()

    //    val coveredCells = parti.bsp.partitions.flatMap(p => parti.bsp.getCellsIn(p.range)).sorted
    //    val distinctCells = coveredCells.distinct.sorted
    //
    //    withClue("covered cells") { coveredCells should contain theSameElementsAs distinctCells }


    //    withClue("number of elements in covered cells") {parti.bsp.partitions.flatMap { p =>
    //      parti.bsp.getCellsIn(p.range)
    //    }.filter(_ < parti.cells.length).map(idx => parti.cells(idx)._2).sum shouldBe cnt }



    //    withClue("number of elements in partitions") {parti.histogram.nonEmptyCells.map(_._2).sum shouldBe cnt}

    rdd.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < parti.numPartitions) }
      } catch {
        case e:IllegalStateException =>

          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4

          parti.partitions.foreach { cell =>

            val xOk = st.getGeo.getCentroid.getX >= cell.range.ll(0) && st.getGeo.getCentroid.getX <= cell.range.ur(0)
            val yOk = st.getGeo.getCentroid.getY >= cell.range.ur(1) && st.getGeo.getCentroid.getY <= cell.range.ur(1)

            println(s"${cell.id} cell: ${cell.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))}  x: $xOk  y: $yOk")
          }

          //        val containingCell = parti.histogram.buckets.values.find(cell => cell._1.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY)))
          //        if(containingCell.isDefined) {
          //          println(s"should be in ${containingCell.get._1.id} which has bounds ${parti.histogram(containingCell.get._1.id)._1.range} and count ${parti.histogram(containingCell.get._1.id)._2}")
          //        } else {
          //          println("No cell contains this point!")
          //        }



          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
      }

    }


  }

  it  should "create real partitions correctly for taxi" taggedAs Slow in {

    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    BSPartitioner.numCellThreshold = -1
    val pc = BSPStrategy(0.1, 100, pointsOnly = true)
    val parti = StarkTestUtils.timing("long taxi version") {
      PartitionerFactory.get(pc,rdd).get
    }
    parti.printPartitions("/tmp/taxipart.wkt")

    rdd.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < parti.numPartitions) }
      } catch {
        case e:IllegalStateException =>

          val xOk = st.getGeo.getCentroid.getX >= parti.minX && st.getGeo.getCentroid.getX <= parti.maxX
          val yOk = st.getGeo.getCentroid.getY >= parti.minY && st.getGeo.getCentroid.getY <= parti.maxY

          parti.partitions.foreach { cell =>

            val xOk = st.getGeo.getCentroid.getX >= cell.range.ll(0) && st.getGeo.getCentroid.getX <= cell.range.ur(0)
            val yOk = st.getGeo.getCentroid.getY >= cell.range.ur(1) && st.getGeo.getCentroid.getY <= cell.range.ur(1)

            println(s"${cell.id} cell: ${cell.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))}  x: $xOk  y: $yOk")
          }

          //        val containingCell = parti.histogram.buckets.values.find (cell => cell._1.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY)))
          //        if(containingCell.isDefined) {
          //          println(s"should be in ${containingCell.get._1.id} which has bounds ${parti.histogram(containingCell.get._1.id)._1.range} and count ${parti.histogram(containingCell.get._1.id)._2}")
          //        } else {
          //          println("No cell contains this point!")
          //        }



          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
      }

    }
  }

  it  should "create real partitions correctly for taxi with sampling" taggedAs Slow in {

    val start = System.currentTimeMillis()
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMax = GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal))

    BSPartitioner.numCellThreshold = -1
    val parti = BSPartitioner(rdd, 0.1, 100, pointsOnly = false, minMax) // disable sampling

    parti.numPartitions should be > 0
    val end = System.currentTimeMillis()
    println(s"sample version: ${end - start} ms")


    rdd.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < parti.numPartitions) }
      } catch {
        case e:IllegalStateException =>

          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4

          parti.partitions.foreach { cell =>

            val xOk = st.getGeo.getCentroid.getX >= cell.range.ll(0) && st.getGeo.getCentroid.getX <= cell.range.ur(0)
            val yOk = st.getGeo.getCentroid.getY >= cell.range.ur(1) && st.getGeo.getCentroid.getY <= cell.range.ur(1)

            println(s"${cell.id} cell: ${cell.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))}  x: $xOk  y: $yOk")
          }

          //          val containingCell = parti.histogram.buckets.values.find (cell => cell._1.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY)))
          //          if(containingCell.isDefined) {
          //            println(s"should be in ${containingCell.get._1.id} which has bounds ${parti.histogram(containingCell.get._1.id)._1.range} and count ${parti.histogram(containingCell.get._1.id)._2}")
          //          } else {
          //            println("No cell contains this point!")
          //          }



          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
      }
    }
  }

  it  should "do yello sample" in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMaxBlocks = GridPartitioner.getMinMax(rddBlocks.map(_._1.getGeo.getEnvelopeInternal))
    BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
    val partiBlocks = BSPartitioner(rddBlocks, sideLength = 0.2, maxCostPerPartition = 100,
      pointsOnly = false, minMax = minMaxBlocks)

    val rddtaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMaxTaxi = GridPartitioner.getMinMax(rddtaxi.map(_._1.getGeo.getEnvelopeInternal))
    val partiTaxi = BSPartitioner(rddtaxi, sideLength = 0.1, maxCostPerPartition = 100,
      pointsOnly = true, minMax = minMaxTaxi)

    val matches = for(t <- partiTaxi.partitions;
                      b <- partiBlocks.partitions
                      if t.extent.intersects(b.extent)) yield (t,b)

    matches.length shouldBe >(0)
    //      val _ = new LiveIndexedSpatialRDDFunctions(rddBlocks, 5).join(rddtaxi, JoinPredicate.CONTAINS, None)
  }

  it  should "correctly partiton random points" in {
    val rdd = sc.textFile("src/test/resources/points.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))

    val parti = BSPartitioner(rdd, 0.5,1000, pointsOnly = true)

    val numparts = parti.numPartitions

    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < numparts)}
      } catch {
        case e:IllegalStateException =>


          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  it  should "correctly partiton random points with sampling" taggedAs Sampling in {
    val rdd = sc.textFile("src/test/resources/points.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))

    val parti = BSPartitioner(rdd, sideLength = 0.5,maxCostPerPartition = 1000, pointsOnly = true)

    val numparts = parti.numPartitions

    withClue("number of partitions"){numparts should be > 0}

    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < numparts)}
      } catch {
        case e:IllegalStateException =>


          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  it  should "correctly join with single point and polygon" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsBSP = BSPartitioner(pointsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsBSP)

    val polygonsBSP = BSPartitioner(polygonsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsBSP)

    val joined = polygonsPart.join(pointsPart, JoinPredicate.CONTAINS)

    joined.collect().length shouldBe 1
  }

  it  should "correctly join with single point and polygon containedby" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsBSP = BSPartitioner(pointsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsBSP)

    val polygonsBSP = BSPartitioner(polygonsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsBSP)

    val joined = pointsPart.join(polygonsPart, JoinPredicate.CONTAINEDBY)

    joined.collect().length shouldBe 1
  }

  it  should "correctly join with single point and polygon intersect point-poly" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsBSP = BSPartitioner(pointsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsBSP)

    val polygonsBSP = BSPartitioner(polygonsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsBSP)

    val joined = pointsPart.join(polygonsPart, JoinPredicate.INTERSECTS)

    joined.collect().length shouldBe 1
  }

  it  should "correctly join with single point and polygon intersect poly-point" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsBSP = BSPartitioner(pointsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsBSP)

    val polygonsBSP = BSPartitioner(polygonsRDD, sideLength = 0.5, maxCostPerPartition = 1000, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsBSP)

    val joined = polygonsPart.join(pointsPart, JoinPredicate.INTERSECTS)

    joined.collect().length shouldBe 1
  }

  it should "contain all taxi points with sampling" taggedAs (Sampling, Slow) in {
    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}



    val partiTaxi = BSPartitioner(rddTaxi, sideLength = 0.1, maxCostPerPartition = 100,
      pointsOnly = true)

    val partedTaxi = rddTaxi.partitionBy(partiTaxi)

    partedTaxi.collect().foreach { case (s,_) =>
      partiTaxi.getPartition(s) should (be >= 0 and be < partiTaxi.numPartitions)
    }
  }

  it should "contain all blocks with sampling" taggedAs (Sampling,Slow) in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
    val parti = BSPartitioner(rddBlocks, sideLength = 0.2, maxCostPerPartition = 100,
      pointsOnly = false)

    val partedBlocks = rddBlocks.partitionBy(parti)

    partedBlocks.collect().foreach { case (s,_) =>
      parti.getPartition(s) should (be >= 0 and be < parti.numPartitions)
    }
  }

  it should "produce same join results with join vs zipJoin" taggedAs Slow in {

    val POSTGRES_RESULT_SIZE = 1311

    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val partiBlocks = BSPStrategy(cellSize = 0.002, maxCost = 1000, pointsOnly = false)
    val partedBlocks = StarkTestUtils.timing("partitioning blocks") {
      rddBlocks.partitionBy(partiBlocks).cache()
    }

    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}//.sample(withReplacement = false, 0.5)

    val partiTaxi = BSPStrategy(cellSize = 0.03, maxCost = 1000,pointsOnly = true)
    val partedTaxi = StarkTestUtils.timing("partitioning taxi") {
      rddTaxi.partitionBy(partiTaxi).cache()
    }

    println(s"#taxis: ${partedTaxi.count()}")
    println(s"#blocks: ${partedBlocks.count()}")


    StarkTestUtils.timing("plain join 1-to-N") {
      val joinRes = partedTaxi.liveIndex(50).join(partedBlocks, JoinPredicate.CONTAINEDBY, oneToMany = true)
      val joinResCnt = joinRes.count()

      withClue("plain join 1-to-N") {
        joinResCnt shouldBe POSTGRES_RESULT_SIZE
      }
      joinResCnt
    }

    StarkTestUtils.timing("plain join 1-to-1") {
      val joinRes = partedTaxi.liveIndex(50).join(partedBlocks, JoinPredicate.CONTAINEDBY, oneToMany = false)
      val joinResCnt = joinRes.count()

      withClue("plain join 1-to-1") {
        joinResCnt shouldBe POSTGRES_RESULT_SIZE
      }
    }

    val partitioner = PartitionerFactory.get(partiTaxi,rddTaxi).get
    val (left, right) = StarkTestUtils.timing("prepare for zip join") {
      SpatialRDD.prepareForZipJoin(leftRDD = partedTaxi, rightRDD = partedBlocks, partitioner = partitioner)
    }

    StarkTestUtils.timing("zip join") {
      val joinRes = left.liveIndex(50).zipJoin(right, JoinPredicate.CONTAINEDBY)
      val joinResCnt = joinRes.count()

      withClue("zip join") {
        joinResCnt shouldBe POSTGRES_RESULT_SIZE
      }
    }
  }


  ignore  should "correctly partition wikimapia" in {
    val rdd = sc.textFile("/home/hg/Documents/uni/stuff/stark/fix_shi/wiki_full.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))

    val parti = BSPartitioner(rdd, 0.5,1000, pointsOnly = true)

    val numparts = parti.numPartitions

    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < numparts)}
      } catch {
        case e:IllegalStateException =>

          //          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          //          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4



          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  ignore should "compute correct join contains" in {
    val points = sc.textFile("/home/hg/Documents/uni/stuff/stark/fix_shi/points10k.csv")
      .map(_.split(","))
      .map(arr => s"POINT(${arr(1)} ${arr(0)})")
      .map(s => (STObject(s),9))


    val wiki = sc.textFile("/home/hg/Documents/uni/stuff/stark/fix_shi/wiki_full.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))


    val jWOPart = wiki.join(points, JoinPredicate.CONTAINS).collect()

    jWOPart should not be empty

    val pointBSP = BSPartitioner(points, 0.5, 100, pointsOnly = true)
    val polyBSP = BSPartitioner(wiki, 0.5, 100, pointsOnly = false)

    val pointsParted = points.partitionBy(pointBSP)
    val polyParted = wiki.partitionBy(polyBSP)

    val jWPart = polyParted.join(pointsParted, JoinPredicate.CONTAINS).collect()

    println(s"no part: ${jWOPart.length}")
    println(s"w/ part: ${jWPart.length}")

    jWOPart should contain theSameElementsAs jWPart
  }


  ignore  should "compute correct join containedby" in {
    val pointsWkt = sc.textFile("/home/hg/Documents/uni/stuff/stark/fix_shi/points10k.csv")
      .map(_.split(","))
      .map(arr => s"POINT(${arr(1)} ${arr(0)})")

    val points = pointsWkt.map(s => (STObject(s),9))


    val wiki = sc.textFile("/home/hg/Documents/uni/stuff/stark/fix_shi/wiki_full.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))


    val jWOPart = points.join(wiki, JoinPredicate.CONTAINEDBY).collect()

    jWOPart should not be empty

    val pointBSP = BSPartitioner(points, 0.5, 100, pointsOnly = true)
    val polyBSP = BSPartitioner(wiki, 0.5, 100, pointsOnly = false)

    val pointsParted = points.partitionBy(pointBSP)
    val polyParted = wiki.partitionBy(polyBSP)

    val jWPart = pointsParted.join(polyParted, JoinPredicate.CONTAINEDBY).collect()

    println(s"no part: ${jWOPart.length}")
    println(s"w/ part: ${jWPart.length}")

    jWOPart should contain theSameElementsAs jWPart
  }

  it should "produce same join results with sampling as without short" taggedAs (Sampling, Slow) in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}.cache()//.sample(withReplacement = false, 0.5)

    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}.cache()//.sample(withReplacement = false, 0.5)


    val noStart = System.currentTimeMillis()
    val joinResNoPart = new LiveIndexedSpatialRDDFunctions(rddBlocks, RTreeConfig(order = 5)).join(rddTaxi, JoinPredicate.CONTAINS, None, oneToMany = true).sortByKey().collect()
    val noEnd = System.currentTimeMillis()

    println(s"no partitioning: ${noEnd - noStart} ms : ${joinResNoPart.length}")


    val taxiPartiNoSample = BSPartitioner(rddTaxi, sideLength = 0.3, maxCostPerPartition = 100, pointsOnly = true)
    val blockPartiNoSample = BSPartitioner(rddBlocks, sideLength = 0.2, maxCostPerPartition = 100, pointsOnly = false)

    val withStart = System.currentTimeMillis()
    val joinResPlain = rddBlocks.partitionBy(blockPartiNoSample).liveIndex(RTreeConfig(order = 5)).join(rddTaxi.partitionBy(taxiPartiNoSample), JoinPredicate.CONTAINS, None, oneToMany = true).sortByKey().collect()
    val withEnd = System.currentTimeMillis()
    println(s"with BSP partitioning: ${withEnd - withStart} ms : ${joinResPlain.length}")

    joinResPlain.length shouldBe joinResNoPart.length

    withClue("join part no sample does not have same results as no partitioning") { joinResPlain should contain theSameElementsAs joinResNoPart }
  }

  it should "correctly partition a file with parallel BSP2" in  {
    val rddRaw = StarkTestUtils.createRDD(sc)

    val partedRDD = rddRaw.partitionBy(BSPStrategy( cellSize = 1, maxCost =  100, pointsOnly =  true))

    val parti = partedRDD.partitioner.get.asInstanceOf[BSPartitioner[STObject]]

    parti.printPartitions("/tmp/bsp2")
    val partitions = parti.partitions
    rddRaw.map(_._1).collect().foreach{ so =>
      val p = NPoint(so.getGeo.getCentroid.getX,so.getGeo.getCentroid.getY)
      partitions.exists(c => c.range.contains(p)) shouldBe true

    }


  }

}

//class BSPartitionerCheck extends Properties("BSPartitioner") {
//
//  val sampleFactor = Gen.choose(0.0, 1.0)
//
//
//
//
//  property("taxi sampling") = forAll(sampleFactor) { (sampleFraction: Double) =>
//
//    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]").setAppName("bsparitioner_check")
//    val sc = new SparkContext(conf)
//
//    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
//      .map { line => line.split(";") }
//      .map { arr => (STObject(arr(1)), arr(0))}.cache()
//    val minMaxTaxi = SpatialPartitioner.getMinMax(rddTaxi, sampleFactor)
//
//    val partiTaxi = BSPartitioner(rddTaxi, sideLength = 0.1, maxCostPerPartition = 100,
//      pointsOnly = true, minMax = minMaxTaxi, sampleFraction = sampleFraction)
//
//    val partedTaxi = rddTaxi.partitionBy(partiTaxi)
//
//    partedTaxi.collect().forall { case (s,_) =>
//      val pIdx = partiTaxi.getPartition(s)
//      pIdx >= 0 && pIdx < partiTaxi.numPartitions
//    }
//  }
//
//  property("blocks sampling") = forAll(sampleFactor) { (sampleFactor: Double) =>
//
//    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]").setAppName("bsparitioner_check")
//    val sc = new SparkContext(conf)
//
//    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
//      .map { line => line.split(";") }
//      .map { arr => (STObject(arr(1)), arr(0))}
//
//    val minMax = SpatialPartitioner.getMinMax(rddBlocks, sampleFactor)
//
//    BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
//    val parti = BSPartitioner(rddBlocks, sideLength = 0.2, maxCostPerPartition = 100,
//      pointsOnly = false, minMax = minMax, sampleFraction = sampleFactor)
//
//    val partedBlocks = rddBlocks.partitionBy(parti)
//
//    partedBlocks.collect().forall { case (s,_) =>
//      val pIdx = parti.getPartition(s)
//      pIdx >= 0 && pIdx < parti.numPartitions
//    }
//
//  }
//
//}
