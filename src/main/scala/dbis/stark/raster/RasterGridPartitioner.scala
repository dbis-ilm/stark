package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.{NPoint, NRectRange}
import dbis.stark.spatial.partitioner.SpatialPartitioner
import org.apache.spark.{Partition, Partitioner}
import org.locationtech.jts.geom.GeometryFactory

import scala.collection.mutable.ListBuffer

case class RasterPartition(index: Int, cell: Int) extends Partition

abstract class RasterPartitioner(val partitionsX: Int, val partitionsY: Int,
                                 val minX: Double, val maxX: Double, val minY: Double, val maxY: Double) extends Partitioner {
  val partitionWidth = (maxX - minX) / partitionsX
  val partitionsHeight = (maxY - minY) / partitionsY

  override def numPartitions = partitionsX * partitionsY

  def idToMBR(id: Int) = {
    val yPos = id / partitionsY
    val xPos = id % partitionsX

    val a = xPos * partitionWidth
    val b = yPos * partitionsHeight

    new MBR(a, a + partitionWidth, b,  b - partitionsHeight)
  }

  def idToNRectRange(id: Int) = {
    val yPos = id / partitionsY
    val xPos = id % partitionsX

    val a = xPos * partitionWidth
    val b = yPos * partitionsHeight

    NRectRange(NPoint(a, b-partitionsHeight), NPoint(a + partitionWidth, b))
  }
}

class RasterGridPartitioner(_partitionsX: Int, partitionsY: Int,
                            minX: Double, maxX: Double, minY: Double, maxY: Double
                           ) extends RasterPartitioner(_partitionsX, partitionsY, minX, maxX, minY, maxY) {

  override def getPartition(key: Any) = {
    val tile = key.asInstanceOf[Tile[_]]
    SpatialPartitioner.getCellId(tile.ulx, tile.uly, minX, minY, maxX, maxY, partitionWidth, partitionsHeight, partitionsX)
  }

  /**
    * Return the partitions that intersect with the spatial component of the provided object
    * @param g The object to apply as a filter
    * @return Returns all partitions that intersect with the given object
    */
  protected[raster] def getPartitionsFor(g: STObject): Array[Partition] = {

    // get the factory to instantiate vector geometry objects
    val factory = new GeometryFactory(g.getGeo.getPrecisionModel, g.getGeo.getSRID)

    var i = 0
    var currId = 0

    val result = ListBuffer.empty[RasterPartition]

    while( i < numPartitions) {
      // get the MBR of partition
      val cellMBR = idToMBR(i)

      // convert the MBR to a geometry
      val cellGeom = factory.toGeometry(cellMBR)

      /* if the partition intersects with the geo
       * create a special RasterPartition, with the ID
       * of the actual partition (i) and a sequence number (currID)
       */
      if(g.getGeo.intersects(cellGeom)) {
        result += RasterPartition(currId, i)
        currId += 1
      }

      i += 1
    }

    result.toArray
  }
}

object RasterGridPartitioner {

  /**
    * Determine min and max value for the raster RDD
    * @param rdd The RDD
    * @return A tuple of (minX, maxX, minY, maxY)
    */
  def getMinMax(rdd: RasterRDD[_]) = rdd.map { t =>
    (t.ulx, t.ulx + t.width, t.uly - t.height, t.uly)
  }.reduce{(t1,t2) =>
    val minX = t1._1 min t2._1
    val maxX = t1._2 max t2._2

    val minY = t1._3 min t2._3
    val maxY = t1._4 max t2._4

    (minX, maxX, minY, maxY)
  }
}