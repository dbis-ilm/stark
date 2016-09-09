package dbis.stark.spatial.indexed.persistent

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.SpatialRDD
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.RTree
import dbis.stark.SpatialObject
import dbis.stark.spatial.Predicates

class PersistedIndexedIntersectionSpatialRDD[G <: SpatialObject : ClassTag, D: ClassTag](
    qry: G, 
    @transient private val prev: RDD[RTree[G,(G,D)]]
  ) extends RDD[RTree[G,(G,D)]](prev) {
  
  private type Index = RTree[G,(G,D)]
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[RTree[G,(G,D)]] = 
    firstParent[Index].iterator(split, context).map { tree =>
      tree.queryRO(qry, Predicates.intersects _)
      tree
    }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
  
  
}