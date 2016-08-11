package dbis.spark.spatial.indexed.persistent

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import dbis.spark.spatial.SpatialRDD
import dbis.spark.spatial.indexed.RTree
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import dbis.spark.spatial.indexed.RTree
import dbis.spark.spatial.indexed.RTree
import dbis.spark.SpatialObject

class PersistedIndexedIntersectionSpatialRDD[G <: SpatialObject : ClassTag, D: ClassTag](
    qry: G, 
    @transient private val prev: RDD[RTree[G,(G,D)]]
  ) extends RDD[(G,D)](prev) {
  
  type Index = RTree[G,(G,D)]
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[(G,D)] = 
    firstParent[Index].iterator(split, context).flatMap { tree =>
      tree.query(qry).filter{ case (g,v) => qry.intersects(g)}
    }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
  
  
}