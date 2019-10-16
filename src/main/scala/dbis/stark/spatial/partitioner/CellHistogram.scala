package dbis.stark.spatial.partitioner

import dbis.stark.spatial.partitioner.GridPartitioner.{getCellBounds, getCellId}
import dbis.stark.spatial.{Cell, NPoint, NRectRange, StarkUtils}
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable
import scala.util.Try

object CellHistogram {
  def empty = CellHistogram()

  def apply( minX:Double, minY:Double, numXCells: Int, numYCells: Int, xLength: Double, yLength: Double): CellHistogram = {
    val buckets = (0 until numYCells).flatMap { y =>
      (0 until numXCells).map { x =>

        val ll = NPoint(minX + x * xLength, minY + y * yLength)
        val ur = NPoint(ll(0) + xLength, ll(1) + yLength)

        val id = y * numXCells + x
        id -> (Cell(id, NRectRange(ll, ur)), 0)
      }
    }

    CellHistogram(mutable.Map(buckets:_*))
  }

  def apply(buckets: Seq[(Int, (Cell, Int))]): CellHistogram = CellHistogram(mutable.Map(buckets:_*))
}

case class CellHistogram protected[partitioner](buckets: mutable.Map[Int, (Cell, Int)] = mutable.Map.empty)
  extends Iterable[(Cell,Int)] {

  override def isEmpty = buckets.isEmpty
  override def nonEmpty = buckets.nonEmpty

  def totalCost: Int = buckets.valuesIterator.map(_._2).sum

  def nonEmptyCells = buckets.values.filter(_._2 > 0)

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
//      println(s"histo: ${buckets.iterator.map(_._2._1.range.wkt).mkString("\n")}")
      throw exception
  }

  def increment(cellId: Int, cell: Cell, o: Envelope, pointsOnly: Boolean): Unit = {
    buckets get cellId match {
      case None =>
        buckets += cellId -> (cell, 1)
//        nonEmptyCells += cellId

      case Some((c, cnt)) =>
        if(!pointsOnly)
          c.extendBy(StarkUtils.fromEnvelope(o))

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

    new CellHistogram(newBuckets)

  }

  def add(pt: Envelope, minX:Double, minY:Double, maxX: Double, maxY: Double, xLength: Double, yLength:Double,
          numXCells: Int, pointsOnly: Boolean): CellHistogram ={
//    val p = StarkUtils.getCenter(pt)
    val p = pt.centre()
    val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)

    val bounds = getCellBounds(cellId, numXCells, xLength, yLength,minX, minY)

    this.increment(cellId, Cell(bounds), pt, pointsOnly)

    this
  }

  override def iterator: Iterator[(Cell, Int)] = this.buckets.valuesIterator
}