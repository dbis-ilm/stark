package dbis.spark.spatial.indexed.live

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import dbis.spark.spatial.indexed.SpatialGridPartition
import dbis.spark.spatial.SpatialPartitioner
import dbis.spark.spatial.Utils

/**
 * An RDD representing a spatial intersection using an internal R-Tree
 * 
 * @param qry The query geometry
 * @param prev The parent RDD 
 */
class LiveIntersectionIndexedSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
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
    val res = result.filter{ case (g,v) => qry.intersects(g) }
    
    res.iterator
  }
  
}