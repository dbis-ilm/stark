package dbis.spark.spatial.plain

import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.OneToOneDependency
import scala.reflect.ClassTag
import com.vividsolutions.jts.geom.Geometry
import scala.collection.JavaConversions._
import org.apache.spark.Logging
import dbis.spark.spatial.indexed.IntersectionIndexedSpatialRDD
import dbis.spark.spatial.indexed.IndexedSpatialRDD
import dbis.spark.spatial.indexed.SpatialPartitioner
import dbis.spark.spatial.indexed.SpatialPartitioner
import dbis.spark.spatial.indexed.SpatialGridPartitioner
import org.apache.spark.rdd.ShuffledRDD

/**
 * A base class for spatial RDD without indexing
 * 
 * @param prev The parent RDD
 */
abstract class SpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    prev: RDD[(G,V)]
  ) extends RDD[(G,V)](prev) {
  
  
  /**
   * We do not repartition our data.
   */
  override protected def getPartitions: Array[Partition] = prev.partitions

  /**
   * Compute an intersection of the elements in this RDD with the given geometry
   */
  def intersect(qry: G): IntersectionSpatialRDD[G,V] = new IntersectionSpatialRDD(qry, this)
  
  def kNN(qry: G, k: Int): KNNSpatialRDD[G,V] = new KNNSpatialRDD(qry, k, this)
  
}

class SpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)]
  ) extends Logging with Serializable {
  

  def intersect(qry: G): RDD[(G,V)] = new IntersectionSpatialRDD(qry, rdd)
  
  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
  
  def index(ppD: Int): IndexedSpatialRDDFunctions[G,V] = index(new SpatialGridPartitioner(ppD, rdd))
  
  def index(partitioner: SpatialPartitioner) = new IndexedSpatialRDDFunctions(partitioner, rdd)
  
  def grid(ppD: Int) = new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner(ppD, rdd))
}

class IndexedSpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner,
    rdd: RDD[(G,V)]
  ) extends Logging with Serializable {
  

  def intersect(qry: G): IndexedSpatialRDD[G,V] = new IntersectionIndexedSpatialRDD(qry, partitioner, rdd)
  
  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
}



object SpatialRDD {
  
  implicit def convertSpatial[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[(G, V)]): SpatialRDDFunctions[G,V] = new SpatialRDDFunctions[G,V](rdd)
  
//  implicit def convertIndexedSpatial[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[(G,V)]): IndexedSpatialRDDFunctions[G,V] = new IndexedSpatialRDDFunctions[G,V](rdd)
  
}



























