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

  def printPartitions(fName: java.nio.file.Path): Unit

  def printPartitions(fName: String): Unit =
    printPartitions(Paths.get(fName))



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
  def getCellsIn(r: NRectRange, sideLength: Double, global: NRectRange, numXCells:Int): IndexedSeq[Int] = {
    val numCells = GridPartitioner.cellsPerDimension(r, sideLength)

    // the cellId of the lower left point of the given range
    val llCellId = GridPartitioner.getCellId(r.ll(0), r.ll(1), global.ll(0), global.ll(1), global.ur(0),
      global.ur(1), sideLength, sideLength, numXCells)

    (0 until numCells(1)).flatMap { i =>
      llCellId + i * numXCells until llCellId + numCells(0) + i * numXCells
    }
  }

//  def getMinMax[G<: STObject,V](rdd: RDD[(G,V)]): (Double, Double, Double, Double) =
//    getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal))

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


  protected[stark] def getCellId(_x: Double, _y: Double, minX: Double, minY: Double, maxX: Double, maxY: Double, xLength: Double, yLength:Double, numXCells: Int): Int = {
    require(_x >= minX && _x <= maxX && _y >= minY && _y <= maxY, s"(${_x},${_y}) out of range!")

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



/**
  * Base class for spatial partitioners
  * @param minX The min value in x dimension
  * @param maxX The max value in x dimension
  * @param minY The min value in y dimension
  * @param maxY The max value in y dimension
  */
abstract class GridPartitioner(
    val minX: Double, var maxX: Double, val minY: Double, var maxY: Double
  ) extends SpatialPartitioner {


  def partitionBounds(idx: Int): Cell
  def partitionExtent(idx: Int): NRectRange


}
