package dbis.spark.spatial.indexed

import dbis.spark.IndexedPartition
import scala.reflect.ClassTag
import com.vividsolutions.jts.geom.Geometry

class IndexedSpatialPartition[G <: Geometry : ClassTag, D: ClassTag](
    val rddId: Long, 
    val slice: Int, 
    val theIndex: RTree[G,D]) extends IndexedPartition(rddId, slice, theIndex) {
  
}