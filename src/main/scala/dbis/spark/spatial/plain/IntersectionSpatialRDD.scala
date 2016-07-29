package dbis.spark.spatial.plain

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.OneToOneDependency
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import dbis.spark.spatial.SpatialRDD

class IntersectionSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    qry: G, 
    @transient private val prev: RDD[(G,V)]
  ) extends SpatialRDD(prev) {
  
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    firstParent[(G,V)].iterator(split, context).filter { case (g,v) => qry.intersects(g) }    
  }
}