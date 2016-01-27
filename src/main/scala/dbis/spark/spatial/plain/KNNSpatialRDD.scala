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

class KNNSpatialRDD[T <: Geometry : ClassTag, V: ClassTag](
    qry: T, k: Int, 
    private val prev: RDD[(T,V)]
  ) extends SpatialRDD(prev) {
  
 /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(T,V)] = {
    iterator(split, context)
      .map { case (g,v) => 
        val d = qry.distance(g)
        (g,v,d)
      }
      .toList
      .sortWith(_._3 < _._3)
      .take(k)
      .map{ case (g,v,d) => (g,v)}
      .toIterator
  }
  

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = prev.partitions

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency(prev))

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient override val partitioner: Option[Partitioner] = None
  
}