package dbis.stark.spatial.indexed.live

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.SpatialGridPartition
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.Utils

class LiveIndexedContainedbySpatialRDD[G <: STObject : ClassTag, V: ClassTag](
    qry: G,
    @transient private val _partitioner: SpatialPartitioner[G,V], 
    @transient private val prev: RDD[(G,V)]
  ) extends IndexedSpatialRDD(_partitioner, prev) {
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    val part = split.asInstanceOf[SpatialGridPartition[G,(G,V)]]

    /* check if the query geometry overlaps with the bounds of this partition
     * if not, the partition does not contain potential query results
     * Therefore, we don't need to build and query the index (which will produce
     * an empty result) 
     */
    if(!qry.getEnvelopeInternal.intersects(Utils.toEnvelope(part.bounds))) {
        // this is not the partition that holds data that might produce results
        logDebug(s"not our part: ${part.bounds}  vs $qry")
        return Iterator.empty
    }
    
    // get and read the shuffled data
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[G, V, V]]
    val iter = SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(G, V)]]
    
    val indexTree = part.theIndex
    
    // Build our index live on-the-fly
    iter.foreach{ case (geom, data) =>
      /* we insert a pair of (geom, data) because we want the tupled
       * structure as a result so that subsequent RDDs build from this 
       * result can also be used as SpatialRDD
       */
      indexTree.insert(geom, (geom,data))
    }
    indexTree.build()
    
    
    
    // now query the index
    val result = indexTree.query(qry)
    
    /* The result of a r-tree query are all elements that 
     * intersect with the MBB of the query region. Thus, 
     * for all result elements we need to check if they
     * really intersect with the actual geometry
     */
    val res = result.filter{ case (g,v) => g.containedBy(qry) }
    
    res.iterator
  }
  
}