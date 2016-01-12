package dbis.spatialspark

import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.OneToOneDependency
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.rdd.MapPartitionsRDD

class SpatialRDD[T:ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends RDD[T](_sc, deps) {
  
  val rtree = new com.vividsolutions.jts.index.strtree.STRtree(3)
  
  
  
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T] = ???
  

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = ???

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient override val partitioner: Option[Partitioner] = None
  
  
  //
  
  def intersect(searchGeom: com.vividsolutions.jts.geom.Geometry): SpatialRDD[T] = {
    ???
  } 
  
  
}