package dbis.stark.spatial.indexed.live

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition

import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.indexed.SpatialGridPartition
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.SpatialGridPartitioner
import dbis.stark.SpatialObject

/**
 * An abstract RDD implementation that uses a live index 
 * 
 * @param _partitioner The partitioner to use for partitioning the underlying data
 * @param oneParent The original RDD 
 */
abstract class IndexedSpatialRDD[G <: SpatialObject : ClassTag, V: ClassTag](
    @transient private val _partitioner: SpatialPartitioner[G,V],
    @transient private val oneParent: RDD[(G,V)]
  ) extends LiveIndexedRDD[G,V](oneParent, _partitioner) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    // the partitioner, since it has to be set in the constructor it's safe to call get here
    val parti = partitioner.get.asInstanceOf[SpatialGridPartitioner[G,V]]
    
    // create the partitions
    Array.tabulate(parti.numPartitions){ idx =>
      val bounds = parti.getCellBounds(idx)
      new SpatialGridPartition[G,V](idx, bounds, new RTree(5))
    }
  }

//  /**
//   * Find all elements in this RDD that intersect with the given geometry.
//   * <br><br>
//   * Before the actual computation is performed, the elements in this RDD a put
//   * into an index structure - on the fly - which then is queried
//   *  
//   * @param qry The query geometry to intersect with the elements in this RDD
//   * @return Returns an RDD that contains all elements that intersect with the given query geometry 
//   */
//  def intersect(qry: G): IndexedSpatialRDD[G,V] = new LiveIndexedIntersectionSpatialRDD(qry, partitioner.get.asInstanceOf[SpatialPartitioner[G,V]], this)
//  
//  def contains(qry: G) = new LiveIndexedContainsSpatialRDD(qry, partitioner.get.asInstanceOf[SpatialPartitioner[G,V]], this)
//  
//  def containedBy(qry: G) = new LiveIndexedContainedbySpatialRDD(qry, partitioner.get.asInstanceOf[SpatialPartitioner[G,V]], this)
//  
//  def kNN(qry: G, k: Int): IndexedSpatialRDD[G,V] = new KNNIndexedSpatialRDD(qry, k, this)
}
