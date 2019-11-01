package dbis.stark.spatial.partitioner

import java.nio.file.{Path, Paths}

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.{Cell, NPoint, NRectRange, StarkUtils}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



trait SpatialPartitioner extends Partitioner {

  private final lazy val empties = Array.fill(numPartitions)(true)

  @inline
  final def isEmpty(id: Int): Boolean = empties(id)

  @inline
  def getPartitionId(key: Any): Int

//  def getAllPartitions(key: Any): List[Int]

  override final def getPartition(key: Any) = {
    val id = getPartitionId(key)
    empties(id) = false
    id
  }

  def getIntersectingPartitionIds(o: STObject):Array[Int]

  def printPartitions(fName: java.nio.file.Path): Unit

  def printPartitions(fName: String): Unit =
    printPartitions(Paths.get(fName))



}

/**
  * Base class for spatial partitioners
  * @param minX The min value in x dimension
  * @param maxX The max value in x dimension
  * @param minY The min value in y dimension
  * @param maxY The max value in y dimension
  */
abstract class GridPartitioner(val partitions: Array[Cell],
                               val minX: Double, var maxX: Double, val minY: Double, var maxY: Double
                              ) extends SpatialPartitioner {

//  private var indexedPartitions: RTree[Cell] = _

  override def getIntersectingPartitionIds(o: STObject):Array[Int] = {

//    if(indexedPartitions == null) {
//      indexedPartitions = new RTree[Cell](10)
//      partitions.foreach(c => indexedPartitions.insert(StarkUtils.toEnvelope(c.extent), c) )
//      indexedPartitions.build()
//    }
//
    val range = StarkUtils.fromGeo(o)
//    partitions.iterator.filter(_.extent.intersects(range)).map(_.id).toArray
//
//    indexedPartitions.queryL(o).map(_.id)

    val buf = new ArrayBuffer[Int](partitions.length / 2)
    var i = 0
    while(i < partitions.length) {
      if(partitions(i).extent intersects range)
        buf += partitions(i).id
      i += 1
    }
    buf.toArray
  }

  def partitionBounds(idx: Int): Cell
  def partitionExtent(idx: Int): NRectRange


}

/**
  * Contains convenience functions used in spatial partitioners
  */
object GridPartitioner {

  var EPS: Double = 1 / 1000000.0

  def writeToFile(strings: Iterable[String], fName: String): Unit =
    writeToFile(strings, Paths.get(fName))

  protected[stark] def writeToFile(strings: Iterable[String], fName: Path): Unit =
    java.nio.file.Files.write(fName, strings.asJava, java.nio.file.StandardOpenOption.CREATE,
      java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)

  def cellsPerDimension(part: NRectRange, sideLength: Double): Array[Int] =
    Array.tabulate(part.dim){dim =>
      math.ceil(part.lengths(dim) / sideLength).toInt}

  /**
    * Determine the IDs of the cells that are contained by the given range
    * @param r The range
    * @return Returns the list of Cell IDs
    */
  def getCellsIn(r: NRectRange, sideLength: Double, universe: NRectRange, numXCells:Int): Array[Int] = {
//    require(r.ur(0) <= universe.ur(0), s"search rect must be smaller than universe in dimension 0! r=${r.ur(0)} vs u=${universe.ur(0)}")
//    require(r.ur(1) <= universe.ur(1), s"search rect must be smaller than universe in dimension 1! r=${r.ur(1)} vs u=${universe.ur(1)}")

    val numCellsPerDimension = GridPartitioner.cellsPerDimension(r, sideLength)

    val numCells = numCellsPerDimension.product

    // the cellId of the lower left point of the given range
    val llCellId = GridPartitioner.getCellId(r.ll, universe, sideLength, sideLength, numXCells)

//    val result = new Array[Int](numCells)
//    var y = 0
//    var pos = 0
//    while(y < numCellsPerDimension(1)) {
//
//      var x = 0
//      while(x < numCellsPerDimension(0)) {
//
//        val cellId = llCellId + y*numXCells + x
//
//        result(pos) = cellId
//        pos += 1
//
//        x += 1
//      }
//      y += 1
//    }


    val result = (0 until numCellsPerDimension(1)).flatMap { i =>
      val row = llCellId / numXCells + i
      val maxPossibleCellIdInLine = (row+1)*numXCells -1
      val calculatedEnd = llCellId + numCellsPerDimension(0)-1 + i * numXCells

      val maxCellIdForRange = math.min(calculatedEnd, maxPossibleCellIdInLine) + 1

      val start = llCellId + i * numXCells

      start until maxCellIdForRange
    }

    result.toArray
  }

  /**
    * Determine the min/max extents of a given RDD
    *
    * Since we use right-open intervals in [[NRectRange]] we add [[EPS]] to the both max values
    *
    * @param rdd The RDD
    * @return Returns a 4-tuple for min/max values in the two dimensions in the form <code>(min-x, max-x, min-y, max-y)</code>
    */
  def getMinMax(rdd: RDD[Envelope]): (Double, Double, Double, Double) = {

//    val theRDD = if(sampleFraction > 0) rdd.sample(withReplacement = false, fraction = sampleFraction) else rdd

    val (minX, maxX, minY, maxY) = rdd.map{ env =>
//      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)

    }.reduce { (oldMM, newMM) =>
      val newMinX = math.min(oldMM._1,newMM._1)
      val newMaxX = math.max(oldMM._2 , newMM._2)
      val newMinY = math.min(oldMM._3 , newMM._3)
      val newMaxY = math.max(oldMM._4 , newMM._4)

      (newMinX, newMaxX, newMinY, newMaxY)
    }

    // do +1 for the max values to achieve right open intervals
    (minX, maxX + EPS, minY, maxY + EPS)
  }

  def getMinMax(samples: Array[MBR]): (Double, Double, Double, Double) = {

    var (minX, maxX, minY, maxY) = {
      val env = samples(0)
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }

    var i = 1
    while(i < samples.length) {

      val env = samples(i)

      minX = math.min(env.getMinX, minX)
      maxX = math.max(env.getMaxX, maxX)

      minY = math.min(env.getMinY, minY)
      maxY = math.max(env.getMaxY, maxY)

      i += 1
    }

    (minX, maxX + EPS, minY, maxY + EPS)

//    val (minX, maxX, minY, maxY) = samples.map{ case (g,_) =>
//      val env = g.getEnvelopeInternal
//      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
//
//    }.reduce { (oldMM, newMM) =>
//      val newMinX = oldMM._1 min newMM._1
//      val newMaxX = oldMM._2 max newMM._2
//      val newMinY = oldMM._3 min newMM._3
//      val newMaxY = oldMM._4 max newMM._4
//
//      (newMinX, newMaxX, newMinY, newMaxY)
//    }
//
//    // do +1 for the max values to achieve right open intervals
//    (minX, maxX + EPS, minY, maxY + EPS)
  }

  protected[stark] def getCellId(p: NPoint, universe: NRectRange, xLength: Double, yLength:Double, numXCells: Int): Int =
    getCellId(p(0), p(1), universe.ll(0), universe.ll(1), universe.ur(0), universe.ur(1), xLength, yLength, numXCells)

  protected[stark] def getCellId(_x: Double, _y: Double, minX: Double, minY: Double, maxX: Double, maxY: Double, xLength: Double, yLength:Double, numXCells: Int): Int = {
    require(_x >= minX && _x <= maxX && _y >= minY && _y <= maxY, s"(${_x},${_y}) out of range: minX=$minX, minY=$minY, maxX=$maxX, maxY=$maxY!")

    val x = math.floor(math.abs(_x - minX) / xLength).toInt
    val y = math.floor(math.abs(_y - minY) / yLength).toInt

    val cellId = y * numXCells + x

    cellId
  }

  /**
    * Compute the bounds of a cell with the given ID
    * @param id The ID of the cell to compute the bounds for
    */
  @inline
  protected[stark] def getCellBounds(id: Int, xCells: Int, xLength: Double, yLength: Double, minX: Double, minY: Double): NRectRange = {

    val dy = id / xCells
    val dx = id % xCells

    val llx = dx * xLength + minX
    val lly = dy * yLength + minY

    val urx = llx + xLength
    val ury = lly + yLength

    NRectRange(NPoint(llx, lly), NPoint(urx, ury))
  }


  def buildHistogram[G <: STObject, V](rdd: RDD[Envelope], pointsOnly: Boolean, numXCells: Int, numYCells: Int,
                                       minX: Double, minY: Double, maxX: Double, maxY: Double,
                                       xLength: Double, yLength:Double): CellHistogram = {


    val m = rdd.map{ env =>
      val p = env.centre()
      val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)

      (cellId, (StarkUtils.fromEnvelope(env),1))
    }.reduceByKey{ case ((lr,lc),(rr,rc)) => (lr.extend(rr), lc + rc) }
      .map{ case (id, (n,c)) =>
        val bounds = getCellBounds(id, numXCells, xLength, yLength,minX, minY)
        (id , (Cell(id,bounds,n),c)) }
      .collect()

    val theMap = mutable.Map.empty[Int, (Cell, Int)]
    theMap.sizeHint(m.length)

    var i = 0
    while(i < m.length) {
      val key = m(i)._1
      val value = m(i)._2
      theMap += key -> value
      i += 1
    }

    var y = 0
    while(y < numYCells) {
      var x = 0
      while(x < numXCells) {

        val id = y * numXCells + x
        if(! theMap.contains(id)) {
          val bounds = getCellBounds(id, numXCells, xLength, yLength, minX, minY)
          theMap += id -> (Cell(id, bounds), 0)

        }
        x += 1
      }
      y += 1
    }

    CellHistogram(theMap)

//    def seq(histo1: CellHistogram, pt: Envelope): CellHistogram =
//      histo1.add(pt, minX, minY, maxX, maxY, xLength, yLength,numXCells, pointsOnly)
//
//    def combine(histo1: CellHistogram, histo2: CellHistogram): CellHistogram = {
//      histo1.combine(histo2,pointsOnly)
//    }
//
//    rdd.aggregate(CellHistogram(minX,minY,numXCells,numYCells,xLength,yLength))(seq, combine)


  }

  def buildGrid(numXCells: Int, numYCells: Int, xLength: Double, yLength: Double, minX: Double, minY: Double): CellHistogram = {

    val gridHisto = mutable.Map.empty[Int, (Cell, Int)]
    var i = 0
    val num = numXCells * numYCells
    while(i < num) {
      val cellBounds = getCellBounds(i, numXCells, xLength, yLength, minX, minY)
      gridHisto += i -> (Cell(i, cellBounds), 0)

      i += 1
    }

    CellHistogram(gridHisto)
  }

}




