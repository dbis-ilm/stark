package dbis.stark.spatial.indexed.persistent

import dbis.stark.SpatialObject
import scala.reflect.ClassTag
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext

class PersistedIndexedWithinDistanceSpatialRDD[G <: SpatialObject : ClassTag, D: ClassTag](
    qry: G, 
    maxDist: Double,
    distFunc: (SpatialObject,SpatialObject) => Double,
    @transient private val prev: RDD[RTree[G,(G,D)]]
  ) extends RDD[(G,D)](prev) {
  
  private type Index = RTree[G,(G,D)]
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[(G,D)] = 
    firstParent[Index].iterator(split, context).flatMap { tree =>
      tree.query(qry).filter{ case (g,_) => distFunc(g,qry) <= maxDist }
    }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
  
}