package dbis.stark.spatial.indexed

import scala.reflect.ClassTag
import dbis.stark.spatial.NRectRange
import dbis.stark.STObject

class SpatialGridPartition[G <: STObject : ClassTag,  D: ClassTag](
    private val _partitionId: Int, 
    private val _bounds: NRectRange, 
    private val _theIndex: RTree[G,D]) extends IndexedPartition(_partitionId, _theIndex) {
  
  protected[spatial] def bounds = _bounds
}