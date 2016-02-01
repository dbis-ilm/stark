package dbis.spark.spatial.indexed.live

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import dbis.spark.spatial.SpatialPartitioner
import dbis.spark.spatial.SpatialGridPartition
import dbis.spark.spatial.indexed.RTree
import dbis.spark.spatial.indexed.SpatialGridPartitioner


abstract class IndexedSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    @transient private val _partitioner: SpatialPartitioner,
    @transient private val oneParent: RDD[(G,V)]
  ) extends LiveIndexedRDD[G,V](oneParent, _partitioner) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    val parti = partitioner.get.asInstanceOf[SpatialGridPartitioner[G,V]]
    Array.tabulate(parti.numPartitions){ idx =>
      val bounds = parti.getCellBounds(idx)
      new SpatialGridPartition[G,V](idx, bounds, new RTree(5))
    }
  }

  
  def intersect(qry: G): IndexedSpatialRDD[G,V] = new LiveIntersectionIndexedSpatialRDD(qry, partitioner.get.asInstanceOf[SpatialPartitioner], this)
  
//  def kNN(qry: T, k: Int): KNNIndexedSpatialRDD[T] = new KNNIndexedSpatialRDD(qry, k, this)
}
