package dbis.spark.spatial.plain

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import dbis.spark.spatial.SpatialRDD

class KNNSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    qry: G, k: Int, 
    private val prev: RDD[(G,V)]
  ) extends RDD[(G,Double,V)](prev) {
  
 /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(G,Double,V)] = {
    firstParent[(G,V)].iterator(split, context)
      .map { case (g,v) => 
        val d = qry.distance(g)
        (g,d,v) // compute and return distance
      }
      .toList
      .sortWith(_._2 < _._2) // on distance
      .take(k) // take only the fist k 
      .toIterator // remove the iterator
  }
  
  /**
   * We do not repartition our data.
   */
  override protected def getPartitions: Array[Partition] = firstParent[(G,V)].partitions
}