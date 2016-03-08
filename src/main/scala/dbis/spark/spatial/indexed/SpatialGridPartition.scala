package dbis.spark.spatial.indexed

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import dbis.spatial.NRectRange

class SpatialGridPartition[G <: Geometry : ClassTag,  D: ClassTag](
    private val _partitionId: Int, 
    private val _bounds: NRectRange, 
    private val _theIndex: RTree[G,D]) extends IndexedPartition(_partitionId, _theIndex) {
  
  protected[spatial] def bounds = _bounds
}