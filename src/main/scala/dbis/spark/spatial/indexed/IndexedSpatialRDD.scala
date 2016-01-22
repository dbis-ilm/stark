package dbis.spark.spatial.indexed

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import dbis.spark.IndexedRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Partitioner

abstract class IndexedSpatialRDD[G <: Geometry : ClassTag, D: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var _deps: Seq[Dependency[(G,D)]],
    @transient private var data: Seq[(G,D)]
  ) extends IndexedRDD[G,D](_sc, _deps) {

  def this(sc: SparkContext, deps: Seq[Dependency[(G,D)]]) = this(sc, deps, Nil)
  
  def this(oneParent: RDD[(G,D)]) = this(oneParent.context, Seq(new OneToOneDependency(oneParent)))

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = {
    
    val rtree = new RTree[G,D](10)
    
    data.foreach { case (geom, item) => rtree.insert(geom, item)}    
    
    rtree.build()
    
    val executors = context.getConf.getInt("", context.defaultParallelism)
    
    val subtrees = rtree.getSubTree(executors)
                        .zipWithIndex
                        .map { case (node, slice) => 
                          val tree = new RTree[G,D](node)
                          new IndexedSpatialPartition(id, slice, tree)
                        }
    
    subtrees.toArray
    
  }

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient override val partitioner: Option[Partitioner] = None
  
  def intersect(qry: G): IntersectionIndexedSpatialRDD[G,D] = new IntersectionIndexedSpatialRDD(qry, this)
  
//  def kNN(qry: T, k: Int): KNNIndexedSpatialRDD[T] = new KNNIndexedSpatialRDD(qry, k, this)
}