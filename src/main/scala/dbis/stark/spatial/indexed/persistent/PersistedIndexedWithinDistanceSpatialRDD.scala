package dbis.stark.spatial.indexed.persistent

import dbis.stark.SpatialObject
import scala.reflect.ClassTag
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import dbis.stark.spatial.Predicates

class PersistedIndexedWithinDistanceSpatialRDD[G <: SpatialObject : ClassTag, D: ClassTag](
    qry: G, 
    maxDist: Double,
    distFunc: (SpatialObject,SpatialObject) => Double,
    @transient private val prev: RDD[RTree[G,(G,D)]]
  ) extends RDD[RTree[G,(G,D)]](prev) {
  
  private type Index = RTree[G,(G,D)]
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[RTree[G,(G,D)]] = 
    firstParent[Index].iterator(split, context).map { tree =>
      /* Here comes some Scala magic:
       * queryRO expects a (predicate) function with two parameters g1 and g2 which will
       * evaluate the desired predicate on the candidates returned by the index query.
       * WithinDistance however has 4 parameters g1,g2, and maxDist and the distance function.
       * 
       * We use function currying to achieve our goal here: We pass a function which is
       * "pre-parameterized" with the maxDist and distFunc values. Now, the queryRO gets
       * the needed function with two parameters for g1 and g2
       * 
       * See http://docs.scala-lang.org/tutorials/tour/currying.html
       */
      tree.queryRO(qry, Predicates.dist(maxDist, distFunc) _)
      tree
    }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent[Index].partitions
  
}