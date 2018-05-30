package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import org.apache.spark.{Partition, TaskContext}
import org.locationtech.jts.geom.GeometryFactory

import scala.reflect.ClassTag

class RasterFilterVectorRDD[U : ClassTag](qry: STObject,
                                          @transient private val _parent: RasterRDD[U]
                           ) extends RasterRDD(_parent) {

  /**
    * Compute the filter
    *
    * @param inputSplit   The current partition
    * @param context The task context
    * @return Returns an iterator over the result elements
    */
  override def compute(inputSplit: Partition, context: TaskContext) = {
    val factory = new GeometryFactory(qry.getGeo.getPrecisionModel, qry.getGeo.getSRID)

    val split = inputSplit match {
      case RasterPartition(_, parent) =>
        firstParent[Tile[U]].partitions(parent)
      case _ => inputSplit
    }


    val qryGeo = qry.getGeo

    firstParent[Tile[U]].iterator(split, context).filter { t =>
      val tileMBR = new MBR(t.ulx, t.ulx + t.width, t.uly - t.height, t.uly)
      val tileGeom = factory.toGeometry(tileMBR)

//      logInfo(s"tileMBR: $tileMBR  tile: $t  qryGeo: $qryGeo")

      qryGeo.intersects(tileGeom) || qryGeo.contains(tileGeom)
    }
  }

  override protected def getPartitions = firstParent.partitioner.map {
      case gp: RasterGridPartitioner =>
        val res = gp.getPartitionsFor(qry)
        logInfo(s"filtered partitions from ${firstParent.partitions.length} to ${res.length}")
        res

      case _ =>
        firstParent.partitions
    }.getOrElse(firstParent.partitions)
}
