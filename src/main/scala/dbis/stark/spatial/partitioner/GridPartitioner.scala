package dbis.stark.spatial.partitioner

import java.nio.file.{Path, Paths}

import dbis.stark.STObject
import dbis.stark.STObject.GeoType
import dbis.stark.spatial.{Cell, NPoint, NRectRange, Utils}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object CellHistogram {
  def empty = CellHistogram()
}

case class CellHistogram(buckets: mutable.Map[Int, (Cell, Int)] = mutable.Map.empty,protected[stark] val nonEmptyCells: mutable.Set[Int] = mutable.Set.empty[Int]) {

  def isEmpty = buckets.isEmpty
  def nonEmpty = buckets.nonEmpty
  def length = buckets.size

  def get(cellId: Int): Option[(Cell, Int)] = buckets get cellId

  def getCountOrElse(cellId: Int)(default: Int) = buckets get cellId match {
    case None => default
    case Some((_,cnt)) => cnt
  }

  def getExtent(cellId: Int): Option[NRectRange] = buckets get cellId match {
    case None => None
    case Some((Cell(_,_,extent), _)) => Some(extent)
  }

  def apply(cellId: Int) = Try(buckets(cellId)) match {
    case scala.util.Success(value) => value
    case scala.util.Failure(exception) =>
      println(s"histo: ${buckets.iterator.map(_._2._1.range.wkt).mkString("\n")}")
      throw exception
  }



  def increment(cellId: Int, cell: Cell, o: GeoType, pointsOnly: Boolean): Unit = {
    buckets get cellId match {
      case None =>
        buckets += cellId -> (cell, 1)
        nonEmptyCells += cellId

      case Some((c, cnt)) =>
        if(!pointsOnly)
          c.extendBy(Utils.fromGeo(o))

        buckets.update(cellId, (c, cnt+1))
    }
  }

  def combine(other: CellHistogram, pointsOnly:Boolean): CellHistogram = {

    val newBuckets = mutable.Map.empty[Int, (Cell, Int)]
    val newNonEmptyCells = mutable.Set.empty[Int]

    buckets.foreach{ case (i, (cell, cnt)) =>
      newBuckets += i -> (cell.clone(), cnt)
    }

    other.buckets.foreach{ case (cellId, (cell, cnt)) =>

      newBuckets get cellId match {
        case None =>
          newBuckets += cellId -> (cell, cnt)
          newNonEmptyCells += cellId

        case Some((c, cnt1)) =>
          val newC = c.clone()
          if(!pointsOnly)
            newC.extendBy(cell.extent)

          newBuckets.update(cellId, (newC, cnt + cnt1))
      }
    }

    new CellHistogram(newBuckets, newNonEmptyCells)

  }


}

trait SpatialPartitioner extends Partitioner {

  private final lazy val empties = Array.fill(numPartitions)(true)

  @inline
  final def isEmpty(id: Int): Boolean = empties(id)

  @inline
  def getPartitionId(key: Any): Int

  override final def getPartition(key: Any) = {
    val id = getPartitionId(key)
    empties(id) = false
    id
  }

  def printPartitions(fName: java.nio.file.Path): Unit

  def printPartitions(fName: String): Unit =
    printPartitions(Paths.get(fName))


  protected[stark] def writeToFile(strings: Iterable[String], fName: Path) =
    java.nio.file.Files.write(fName, strings.asJava, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
}


/**
  * Contains convenience functions used in spatial partitioners
  */
object GridPartitioner {

  var EPS: Double = 1 / 1000000.0


  /**
    * Determine the min/max extents of a given RDD
    *
    * Since we use right-open intervals in [[NRectRange]] we add [[EPS]] to the both max values
    *
    * @param rdd The RDD
    * @tparam G The data type representing spatio-temporal data
    * @tparam V The type for payload data
    * @return Returns a 4-tuple for min/max values in the two dimensions in the form <code>(min-x, max-x, min-y, max-y)</code>
    */
  def getMinMax[G <: STObject, V](rdd: RDD[(G,V)]): (Double, Double, Double, Double) = {

//    val theRDD = if(sampleFraction > 0) rdd.sample(withReplacement = false, fraction = sampleFraction) else rdd

    val (minX, maxX, minY, maxY) = rdd.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)

    }.reduce { (oldMM, newMM) =>
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4

      (newMinX, newMaxX, newMinY, newMaxY)
    }

    // do +1 for the max values to achieve right open intervals
    (minX, maxX + EPS, minY, maxY + EPS)
  }

  def getMinMax[G <: STObject, V](samples: Iterator[(G,V)]): (Double, Double, Double, Double) = {

    val (minX, maxX, minY, maxY) = samples.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)

    }.reduce { (oldMM, newMM) =>
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4

      (newMinX, newMaxX, newMinY, newMaxY)
    }

    // do +1 for the max values to achieve right open intervals
    (minX, maxX + EPS, minY, maxY + EPS)
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


  def buildHistogram[G <: STObject, V](rdd: RDD[(G,V)], pointsOnly: Boolean, numXCells: Int, numYCells: Int,
                                       minX: Double, minY: Double, maxX: Double, maxY: Double,
                                       xLength: Double, yLength:Double): CellHistogram = {

    def seq(histo1: CellHistogram, pt: (G,V)): CellHistogram = {

      val p = Utils.getCenter(pt._1.getGeo)
      val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)

      val bounds = getCellBounds(cellId, numXCells, xLength, yLength,minX, minY)

      histo1.increment(cellId, Cell(bounds), pt._1.getGeo, pointsOnly)

      histo1
    }

    def combine(histo1: CellHistogram, histo2: CellHistogram): CellHistogram = {
      histo1.combine(histo2,pointsOnly)
    }

//    val histo = buildGrid(numXCells,numYCells, xLength, yLength, minX,minY)

    rdd.aggregate(CellHistogram.empty)(seq, combine)

    /* fill the array. If with extent, we need to keep the exent of each element and combine it later
     * to create the extent of a cell based on the extents of its contained objects
     */
//    if(pointsOnly) {
//      rdd.map{ case (g,_) =>
//        val p = Utils.getCenter(g.getGeo)
//
//        val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)
//
//        (cellId, 1)
//      }
//      .reduceByKey(_ + _)
////      .collect
//      .cache()
//      .toLocalIterator
//      .foreach{ case (cellId, cnt) =>
//        histo(cellId) = (histo(cellId)._1, cnt)
//      }
//
//
//    } else {
//      rdd.map { case (g, _) =>
//        val p = Utils.getCenter(g.getGeo)
////        val env = g.getEnvelopeInternal
////        val extent = NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))
//        val extent = Utils.fromGeo(g.getGeo)
//        val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)
//
//        (cellId,(1, extent))
//      }
//      .reduceByKey{ case ((lCnt, lExtent), (rCnt, rExtent)) =>
//        val cnt = lCnt + rCnt
//
//        val extent = lExtent.extend(rExtent)
//
//        (cnt, extent)
//
//      }
////        .collect
//      .cache()
//      .toLocalIterator
//      .foreach{case (cellId, (cnt,ex)) =>
//        histo(cellId) = (Cell(cellId, histo(cellId)._1.range, ex) , cnt)
//      }
//    }
//    histo

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
