package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.partitioner.SpatialPartitioner
import org.apache.spark.{Partition, Partitioner}
import org.locationtech.jts.geom.GeometryFactory

import scala.collection.mutable.ListBuffer

case class RasterPartition(index: Int, cell: Int) extends Partition

class RasterGridPartitioner(partitionsX: Int, partitionsY: Int,
                            minX: Double, maxX: Double, minY: Double, maxY: Double
                           ) extends Partitioner {

  val partitionWidth = (maxX - minX) / partitionsX
  val partitionsHeight = (maxY - minY) / partitionsY

  override def numPartitions = partitionsX * partitionsY

  override def getPartition(key: Any) = {
    val tile = key.asInstanceOf[Tile[_]]

    SpatialPartitioner.getCellId(tile.ulx, tile.uly, minX, minY, maxX, maxY, partitionWidth, partitionsHeight, partitionsX)

  }

  protected[raster] def getPartitionsFor(g: STObject): Array[Partition] = {

    val factory = new GeometryFactory(g.getGeo.getPrecisionModel, g.getGeo.getSRID)

    var i = 0
    var currId = 0

    val result = ListBuffer.empty[RasterPartition]

    while( i < numPartitions) {
      val yPos = i / partitionsY
      val xPos = i % partitionsX

      val a = xPos * partitionWidth
      val b = yPos * partitionsHeight

      val cellMBR = new MBR(a, a + partitionWidth, b,  b - partitionsHeight)

      val cellGeom = factory.toGeometry(cellMBR)

      if(g.getGeo.intersects(cellGeom)) {
        result += RasterPartition(currId, i)
        currId += 1
      }

      i += 1
    }

    result.toArray
  }
}
