package dbis.stark.spatial.plain

import dbis.stark.spatial.SpatialRDD
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import dbis.stark.STObject

class ContainedBySpatialRDD[G <: STObject : ClassTag, V: ClassTag](
    qry: G,
    @transient private val prev: RDD[(G,V)]) extends SpatialRDD[G,V](prev) {
  
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    firstParent[(G,V)].iterator(split, context).filter { case (g,v) => qry.contains(g) }    
  }
  
}