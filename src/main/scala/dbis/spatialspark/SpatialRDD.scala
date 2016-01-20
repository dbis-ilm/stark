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
import com.vividsolutions.jts.geom.Geometry
import scala.collection.JavaConversions._



class ExtendedRDD[X] private (rdd: RDD[X]) {
  def makeSpatial[Y <: Geometry : ClassTag](f: X => Y): SpatialRDD[Y] = new SpatialRDD(rdd.map(f))
}

object ExtendedRDD {
  
  implicit def convertExtended[X](rdd: RDD[X]): ExtendedRDD[X] = new ExtendedRDD[X](rdd)
  
  implicit def convertSpatial[X <: Geometry : ClassTag](rdd: RDD[X]): SpatialRDD[X] = new SpatialRDD[X](rdd) 
  
}

/*
 * SEE UnionRDD for some useful implementation ideas
 * - they create their own Partition type and instances thereof in getPartitions
 * - in compute the partition is casted to this type 
 */


class SpatialRDD[T <: Geometry :ClassTag](
//    @transient private var _sc: SparkContext,
//    @transient private var deps: Seq[Dependency[_]]
    prev: RDD[T]
  ) extends RDD[T](prev) {
  
  
  println("bllaaaaaaaaa")
  
  
//  def this(@transient oneParent: RDD[_]) =
//    this(oneParent.context , List(new OneToOneDependency(oneParent)))
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T] = firstParent[T].iterator(split, context)
  

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = firstParent.partitions

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

  def intersect(qry: T): IntersectionSpatialRDD[T] = new IntersectionSpatialRDD(qry, this)
  
  def kNN(qry: T, k: Int): KNNSpatialRDD[T] = new KNNSpatialRDD(qry, k, this)
  
}

























