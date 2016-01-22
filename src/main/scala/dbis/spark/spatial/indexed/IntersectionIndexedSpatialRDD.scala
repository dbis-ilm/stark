package dbis.spark.spatial.indexed

import com.vividsolutions.jts.geom.Geometry

import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.OneToOneDependency
import org.apache.spark.Dependency
import org.apache.spark.Partitioner

class IntersectionIndexedSpatialRDD[G <: Geometry : ClassTag, D: ClassTag](
    qry: G, 
    prev: IndexedSpatialRDD[G,D]
  ) extends IndexedSpatialRDD(prev) {
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    
    val indexTree = split.asInstanceOf[IndexedSpatialPartition[G,D]].theIndex 
    
    indexTree.query(qry)
  }
  


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