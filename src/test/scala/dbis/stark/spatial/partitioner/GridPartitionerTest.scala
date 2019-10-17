package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import dbis.stark.{STObject, StarkKryoRegistrator}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.util.Random

class GridPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  def makeHisto(cols: Int, rows: Int) = {
    val buckets = mutable.Map.empty[Int, (Cell, Int)]

    var i = -1
    for(y <- 0 to rows;
        x <- 0 to cols) {
      i += 1
      buckets += i -> (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    CellHistogram(buckets)
  }

  private def createCells(sideLength: Double, cost: Int = 10, numXCells: Int, numYCells: Int, llStartX: Double, llStartY: Double) = {

    val histo = {

      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>

          val ll = NPoint(llStartX + x*sideLength, llStartY+y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)

          val id = y * numXCells + x
          (Cell(id, NRectRange(ll,ur)), Random.nextInt(cost))
        }
      }.toArray

    }

    val ll = NPoint(llStartX, llStartY)
    val ur = NPoint(llStartX+numXCells*sideLength, llStartY+numYCells*sideLength)
    val whole = NRectRange(ll, ur)

    (ll,ur,whole, histo)
  }

  "The GridPartitioner bases" should "compute correct cells in range covering complete space" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    GridPartitioner.getCellsIn(startRange,1,startRange,4) should contain only (0 to 15 :_*)

  }

//  it should "compute correct cells in range covering greater space" in {
//
//    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
//
//    val theRange = NRectRange(NPoint(0, 0),NPoint(20,20))
//
//    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only (0 to 15 :_*)
//  }

  it should "compute correct cells in range covering a single cell" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val theRange = NRectRange(NPoint(2, 2),NPoint(3,3))

    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only 10
  }

  it should "compute correct cells in range covering multiple cells" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only (Seq(6,7,10,11):_*)
  }

  it should "get the correct extent even for points" in {
    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val sideLength = 1
    val numXCells = 4

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    val extent = CostBasedPartitioner.extentForRange(theRange,sideLength ,startRange,numXCells ,histo)

    extent shouldBe theRange

  }

  it should "estimate costs correctly" in {
    val histo = makeHisto(3,3)

//    println(histo.buckets.values.mkString("\n"))

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val sideLength = 1.0
//    val bspTask = new SplitTask(startRange, histo,
//      sideLength, maxCostPerPartition = 3, pointsOnly = false )

    histo.buckets.foreach{ case (_,(cell,_)) =>
      CostBasedPartitioner.costEstimation(cell.range,sideLength, startRange,4,histo) shouldBe 1
    }

    CostBasedPartitioner.costEstimation(startRange,sideLength,startRange,4,histo) shouldBe 16

    CostBasedPartitioner.costEstimation(NRectRange(NPoint(0.5, 0.5), NPoint(1,1)),sideLength,startRange,4,histo) shouldBe 1

  }

  it should "compute correct total costs" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX,maxX,minY,maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val cellHistogram = GridPartitioner.buildHistogram(rdd,pointsOnly = true,numcells(0),numcells(1),
      minX, minY, maxX,maxY,sideLength,sideLength)

    val r1 = NRectRange(NPoint(-74.0180435180664,0.0),NPoint(-37.018043518066406,maxY))
    val r2 = NRectRange(NPoint(-37.018043518066406,0.0),NPoint(0.08195648193360228,maxY)) //40.1

    val cost1 = CostBasedPartitioner.costEstimation(r1, sideLength, universe,numcells(0),cellHistogram)
    val cost2 = CostBasedPartitioner.costEstimation(r2, sideLength, universe,numcells(0),cellHistogram)

    val costUniverse = CostBasedPartitioner.costEstimation(universe, sideLength, universe,numcells(0),cellHistogram)
    val total = cellHistogram.totalCost

    withClue("universe costs ") { costUniverse shouldBe total }
    withClue("sum costs ") { cost1 + cost2 shouldBe total}
  }

  it should "find correct cells in ranges without overlaps" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX,maxX,minY,maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val r1 = NRectRange(NPoint(-74.0180435180664,0.0),NPoint(-37.018043518066406,maxY))
    val r2 = NRectRange(NPoint(-37.018043518066406,0.0),NPoint(0.08195648193360228,maxY))

    val cells1 = GridPartitioner.getCellsIn(r1,sideLength,universe,numcells(0)).toSet
    val cells2 = GridPartitioner.getCellsIn(r2,sideLength,universe,numcells(0)).toSet

    cells1.intersect(cells2).toList.sorted shouldBe empty
  }

  it should "return only cells that are contained in requested range r1" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX,maxX,minY,maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val r1 = NRectRange(NPoint(-74.0180435180664,0.0),NPoint(-37.018043518066406,maxY))

    val cells1 = GridPartitioner.getCellsIn(r1,sideLength,universe,numcells(0)).toSet

    cells1 should not be empty

    cells1.toList.sorted.foreach{ cId =>
      val bounds = GridPartitioner.getCellBounds(cId,numcells(0),sideLength,sideLength,minX,minY)

      withClue(s"cell $cId not in r1"){r1.intersects(bounds) shouldBe true}
    }
  }

  it should "return only cells that are contained in requested range r2" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX,maxX,minY,maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val r2 = NRectRange(NPoint(-37.018043518066406,0.0),NPoint(0.08195648193360228,maxY))

    val cells2 = GridPartitioner.getCellsIn(r2,sideLength,universe,numcells(0)).toSet

    cells2 should not be empty

    cells2.toList.sorted.foreach{ cId =>
      val bounds = GridPartitioner.getCellBounds(cId,numcells(0),sideLength,sideLength,minX,minY)
      withClue(s"cell $cId not in r2"){r2.intersects(bounds) shouldBe true}
    }
  }

  it should "find cells that make up requested space r1" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX,maxX,minY,maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val r1 = NRectRange(NPoint(-74.0180435180664,0.0),NPoint(-37.018043518066406,40.1))

    val cells1 = GridPartitioner.getCellsIn(r1,sideLength,universe,numcells(0)).toSet

    val cell1Extent = cells1.map{cId =>
      GridPartitioner.getCellBounds(cId,numcells(0),sideLength,sideLength,minX,minY)
    }.reduce(_.extend(_))

    (cell1Extent ~= r1) shouldBe true
  }

  it should "find cells that make up requested space r2" in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0)) }
      .map(_._1.getGeo.getEnvelopeInternal).cache()

    val sideLength = 0.1

    val (minX, maxX, minY, maxY) = GridPartitioner.getMinMax(rdd)

    val universe = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

    val numcells = GridPartitioner.cellsPerDimension(universe, sideLength)

    val r2 = NRectRange(NPoint(-37.018043518066406, 0.0), NPoint(0.08195648193360228, 40.1))

    val cells2 = GridPartitioner.getCellsIn(r2, sideLength, universe, numcells(0)).toSet


    val cell2Extent = cells2.map { cId =>
      GridPartitioner.getCellBounds(cId, numcells(0), sideLength, sideLength, minX, minY)
    }.reduce(_.extend(_))

    (cell2Extent ~= r2) shouldBe true

  }

  it should "find correct cells per dimension" in {
    val cell1 = Cell(0,
      range = NRectRange(NPoint(-4,-4), NPoint(0,0)),
      extent = NRectRange(NPoint(-7,-5), NPoint(0,1)))

    val cell2 = Cell(2,
      range = NRectRange(NPoint(0,-4), NPoint(4,0)),
      extent = NRectRange(NPoint(-1,-4), NPoint(3,-1)))

    val cell3 = Cell(1,
      range = NRectRange(NPoint(-4,0), NPoint(0,4)),
      extent = NRectRange(NPoint(-2,3), NPoint(2,6)))

    val cell4 = Cell(3,
      range = NRectRange(NPoint(0,0), NPoint(4,4)),
      extent = NRectRange(NPoint(-1,-1), NPoint(6,6)))

    val ll = NPoint(-4,-4)
    val ur = NPoint(4,4)
    val sideLength = 4

    val start = Cell(NRectRange(ll,ur))

    withClue("start") { GridPartitioner.cellsPerDimension(start.range,sideLength) should contain theSameElementsAs List(2,2) }
    withClue("cell1 +2") { GridPartitioner.cellsPerDimension(cell1.range.extend(cell2.range),sideLength) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell3 +4") { GridPartitioner.cellsPerDimension(cell3.range.extend(cell4.range),sideLength) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell1 +3") { GridPartitioner.cellsPerDimension(cell1.range.extend(cell3.range),sideLength) should contain theSameElementsInOrderAs List(1,2) }
    withClue("cell2 +4") { GridPartitioner.cellsPerDimension(cell2.range.extend(cell4.range),sideLength) should contain theSameElementsInOrderAs List(1,2) }

  }
}
