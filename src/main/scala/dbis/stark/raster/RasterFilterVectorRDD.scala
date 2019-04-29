package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class RasterFilterVectorRDD[U : ClassTag](qry: STObject,
                                          @transient private val _parent: RasterRDD[U],
                                          predicate: JoinPredicate,
                                          pixelDefault: U
                                         )(implicit ord: Ordering[U]) extends RasterRDD(_parent) {

  private val predicateFunc = JoinPredicate.spatialPredicateFunction(predicate)
  private val isIntersects = predicate == JoinPredicate.INTERSECTS

  /**
    * Compute the filter
    *
    * @param inputSplit   The current partition
    * @param context The task context
    * @return Returns an iterator over the result elements
    */
  override def compute(inputSplit: Partition, context: TaskContext) = {

    val split = inputSplit match {
      case RasterPartition(_, parent) =>
        firstParent[Tile[U]].partitions(parent)
      case _ => inputSplit
    }


    firstParent[Tile[U]].iterator(split, context).filter { t =>
        val tileGeom = RasterUtils.tileToGeo(t)
        predicateFunc(tileGeom, qry)
      }
      .map{t =>
        RasterUtils.getPixels(t, qry.getGeo, isIntersects, pixelDefault )
      }
  }

  override protected def getPartitions = firstParent.partitioner.map {
      case gp: RasterGridPartitioner =>
        val res = gp.getIntersectingPartitions(qry)
//        logInfo(s"filtered partitions from ${firstParent.partitions.length} to ${res.length}")
        res

      case _ =>
        firstParent.partitions
    }.getOrElse(firstParent.partitions)
}
