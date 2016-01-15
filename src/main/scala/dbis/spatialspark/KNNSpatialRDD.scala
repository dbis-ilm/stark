package dbis.spatialspark

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partitioner
import scala.collection.mutable.ListBuffer

class KNNSpatialRDD[T <: Geometry : ClassTag](qry: T, k: Int, prev: SpatialRDD[T]) extends SpatialRDD[T](prev) {
  
 /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T]
      .iterator(split, context)
      .map { e => 
        val d = qry.distance(e)
        (e,d)
      }
      .toList
      .sortWith(_._2 < _._2)
      .take(k)
      .map(_._1)
      .toIterator
  }
  

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = firstParent.partitions

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