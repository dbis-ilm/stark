package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.locationtech.jts.geom.GeometryFactory

class RasterFilterVectorRDD(qry: STObject,
                            @transient private val _parent: RDD[Tile]
                           ) extends RDD[Tile](_parent) {

  /**
    * Compute the filter
    * @param split The current partition
    * @param context The task context
    * @return Returns an iterator over the result elements
    */
  override def compute(split: Partition, context: TaskContext) = {

    val factory = new GeometryFactory(qry.getGeo.getPrecisionModel, qry.getGeo.getSRID)

    val qryGeo = qry.getGeo

    firstParent[Tile].iterator(split, context).filter { t =>
      val tileMBR = new MBR(t.ulx, t.ulx + t.width, t.uly - t.height, t.uly)
      val tileGeom = factory.toGeometry(tileMBR)

      qryGeo.intersects(tileGeom) || qryGeo.contains(tileGeom)
    }
  }

  override protected def getPartitions = partitioner.map {
    case gp: RasterGridPartitioner =>
      gp.getPartitionsFor(qry)

    case _ => firstParent.partitions
  }.getOrElse(firstParent.partitions)
}
