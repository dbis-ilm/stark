package dbis.stark.raster

import java.nio.file.Path

import dbis.stark.STObject
import dbis.stark.spatial.partitioner.GridPartitioner
import dbis.stark.spatial.{Cell, StarkUtils}
import org.apache.spark.Partition

import scala.collection.mutable.ListBuffer

case class RasterPartition(index: Int, cell: Int) extends Partition

abstract class RasterPartitioner(val partitionsX: Int, val partitionsY: Int,
                                 minX: Double, maxX: Double, minY: Double, maxY: Double) extends GridPartitioner(Array.empty, minX, maxX, minY, maxY) {
  val partitionWidth = (maxX - minX) / partitionsX
  val partitionsHeight = (maxY - minY) / partitionsY

//  println(s"partition width: $partitionWidth  height: $partitionsHeight")

  override def numPartitions = partitionsX * partitionsY

  @inline
  def idToMBR(id: Int) = StarkUtils.toEnvelope(idToNRectRange(id))

  @inline
  def idToNRectRange(id: Int) =
    GridPartitioner.getCellBounds(id, partitionsX, partitionWidth, partitionsHeight, minX, minY)

}

class RasterGridPartitioner(_partitionsX: Int, partitionsY: Int,
                            minX: Double, maxX: Double, minY: Double, maxY: Double
                           ) extends RasterPartitioner(_partitionsX, partitionsY, minX, maxX, minY, maxY) {

  override def getPartitionId(key: Any) = {
    val tile = key.asInstanceOf[Tile[_]]
    val (centerx, centery) = tile.center

    val partId = GridPartitioner.getCellId(centerx, centery, minX, minY, maxX, maxY, partitionWidth, partitionsHeight, partitionsX)

    require(partId >= 0 && partId < numPartitions, s"invalid $partId  (0 .. $numPartitions)")

    partId
  }

  /**
    * Return the partitions that intersect with the spatial component of the provided object
    * @param g The object to apply as a filter
    * @return Returns all partitions that intersect with the given object
    */
  protected[raster] def getIntersectingPartitions(g: STObject): Array[Partition] = {

    var i = 0
    var currId = 0

    val gMbr = g.getGeo.getEnvelopeInternal

    val result = ListBuffer.empty[RasterPartition]

    while( i < numPartitions) {
      // get the MBR of partition
      val cellMBR = idToMBR(i)

      /* if the partition intersects with the geo
       * create a special RasterPartition, with the ID
       * of the actual partition (i) and a sequence number (currID)
       */
      if(cellMBR.intersects(gMbr)) {
        result += RasterPartition(currId, i)
        currId += 1
      }

      i += 1
    }

    result.toArray
  }

  override def partitionBounds(idx: Int) = Cell(idToNRectRange(idx))

  override def partitionExtent(idx: Int) = idToNRectRange(idx)

  override def printPartitions(fName: Path): Unit = {
    val strings = (0 until numPartitions).iterator.map(i => partitionExtent(i).wkt).toIterable
    GridPartitioner.writeToFile(strings, fName)
  }
}

object RasterGridPartitioner {

  /**
    * Determine min and max value for the raster RDD
    * @param rdd The RDD
    * @return A tuple of (minX, maxX, minY, maxY)
    */
  def getMinMax(rdd: RasterRDD[_]) = rdd.map { t =>
    (t.ulx, t.ulx + t.width*t.pixelWidth, t.uly - t.height*t.pixelWidth, t.uly)
  }.reduce{(t1,t2) =>
    val minX = t1._1 min t2._1
    val maxX = t1._2 max t2._2

    val minY = t1._3 min t2._3
    val maxY = t1._4 max t2._4

    (minX, maxX, minY, maxY)
  }
}