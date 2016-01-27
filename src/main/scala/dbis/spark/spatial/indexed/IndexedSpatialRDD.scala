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
import org.apache.spark.deploy.SparkSubmit


abstract class IndexedSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    @transient private val oneParent: RDD[(G,V)]
  ) extends IndexedRDD[G,V](oneParent, new RTreePartitioner(10, oneParent)) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = {
    val part = partitioner match {
      case None => println("create new"); new RTreePartitioner(10, oneParent)
      case Some(p) => println(s"resuse $p"); p
    }
    Array.tabulate(part.numPartitions)(idx => new IndexedSpatialPartition[G,V](idx))
  }


  /** Optionally overridden by subclasses to specify how they are partitioned. */
  
  def intersect(qry: G): IndexedSpatialRDD[G,V] = new IntersectionIndexedSpatialRDD(qry, this)
  
//  def kNN(qry: T, k: Int): KNNIndexedSpatialRDD[T] = new KNNIndexedSpatialRDD(qry, k, this)
}
