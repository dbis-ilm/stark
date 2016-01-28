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
    // TODO: make partitions per dimension configurable
  ) extends IndexedRDD[G,V](oneParent, new SpatialGridPartitioner(2, oneParent)) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    val parti = partitioner.get
    Array.tabulate(parti.numPartitions)(idx => new IndexedSpatialPartition[G,V](idx, new RTree(5)))
  }

  
  def intersect(qry: G): IndexedSpatialRDD[G,V] = new IntersectionIndexedSpatialRDD(qry, this)
  
//  def kNN(qry: T, k: Int): KNNIndexedSpatialRDD[T] = new KNNIndexedSpatialRDD(qry, k, this)
}
