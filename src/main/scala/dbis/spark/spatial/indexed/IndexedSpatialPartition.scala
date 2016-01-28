package dbis.spark.spatial.indexed

import dbis.spark.IndexedPartition
import scala.reflect.ClassTag
import com.vividsolutions.jts.geom.Geometry

class IndexedSpatialPartition[G <: Geometry : ClassTag, D: ClassTag](
    private val _partitionId: Int, 
    private val _theIndex: RTree[G,D]) extends IndexedPartition(_partitionId, _theIndex) {
  
  
}