package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject
import scala.reflect.ClassTag
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi

class PersistedIndexedKNNSpatialRDD[G <: STObject : ClassTag, D: ClassTag](
    qry: G, 
    k: Int,
    @transient private val prev: RDD[RTree[G,(G,D)]]
  ) extends RDD[(G,D)](prev) {
  
  private type Index = RTree[G,(G,D)]
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[(G,D)] = 
    firstParent[Index].iterator(split, context).flatMap { tree =>
      tree.kNN(qry, k)
    }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
}