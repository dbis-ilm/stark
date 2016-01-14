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

object Operation extends Enumeration {
  type Operation = Value
  val INTERSECT, KNN = Value
  
}

class ExtendedRDD[X] private (rdd: RDD[X]) {
  def makeSpatial[Y: ClassTag](f: X => Y): SpatialRDD[Y] = ???
}

object ExtendedRDD {
  
  implicit def convertSpatial[X](rdd: RDD[X]): ExtendedRDD[X] = new ExtendedRDD[X](rdd)
  
}

/*
 * SEE UnionRDD for some useful implementation ideas
 * - they create their own Partition type and instances thereof in getPartitions
 * - in compute the partition is casted to this type 
 */

import Operation._

class SpatialRDD[T:ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends RDD[T](_sc, deps) {
  
  private val rtree = new com.vividsolutions.jts.index.strtree.STRtree(3)
  
  private var op: Option[Operation] = None
  private var searchGeom: Option[Geometry] = None
  private var k: Int = -1
  
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    
    /* Actually this cannot happen, can it?
     * In case there is no operation set, simply pass the input split
     */
    if(op.isEmpty) 
      throw new IllegalStateException("No operation set")
      
    val o = op.get
    val qry = searchGeom.get
    
    val result = op match {
      case INTERSECT => 
        val res = rtree.query(qry.getEnvelopeInternal)
        res.map{ obj => obj.asInstanceOf[T]}.toIterator
      case _ => ???
    }
    
    result
  }
  

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
  
  
  def intersect(searchGeom: Geometry): SpatialRDD[T] = {
    op = Some(Operation.INTERSECT)
    this.searchGeom = Some(searchGeom)
    this
  } 
  
  def kNN(ref: Geometry, k: Int): SpatialRDD[T] = {
    op = Some(Operation.KNN)
    this.searchGeom = Some(ref)
    this.k = k
    this
  }
  
  
}