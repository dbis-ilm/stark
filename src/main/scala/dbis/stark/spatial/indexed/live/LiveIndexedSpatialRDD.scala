package dbis.stark.spatial.indexed.live

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition

import dbis.stark.STObject
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.SpatialPartition

/**
 * An abstract RDD implementation that uses a live index 
 * 
 * @param _partitioner The partitioner to use for partitioning the underlying data
 * @param oneParent The original RDD 
 */
abstract class IndexedSpatialRDD[G <: STObject : ClassTag, V: ClassTag](
    @transient private val _partitioner: SpatialPartitioner[G,V],
    @transient private val oneParent: RDD[(G,V)]
  ) extends LiveIndexedRDD[G,V](oneParent, _partitioner) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    // the partitioner, since it has to be set in the constructor it's safe to call get here
    val parti = partitioner.get.asInstanceOf[SpatialPartitioner[G,V]]
    
    // create the partitions
    Array.tabulate(parti.numPartitions){ idx =>
      val bounds = parti.partitionBounds(idx)
      new SpatialPartition(idx, bounds.extent)
    }
  }
}
