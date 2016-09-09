package dbis.stark.spatial.indexed.persistent

import dbis.stark.SpatialObject
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import dbis.stark.spatial.Predicates

class PersistedIndexedContainedbySpatialRDD[G <: SpatialObject : ClassTag, V: ClassTag](
    qry: G,
    @transient private val prev: RDD[RTree[G,(G,V)]]
  ) extends RDD[RTree[G,(G,V)]](prev) {
  
  private type Index = RTree[G,(G,V)]
  
  def compute(split: Partition, context: TaskContext): Iterator[RTree[G,(G,V)]] = { 
    
    firstParent[Index].iterator(split, context).map { tree =>
      tree.queryRO(qry, Predicates.containedby _)
      tree
    }
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
  
}