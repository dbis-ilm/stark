package dbis.spark.spatial.indexed

import dbis.spark.IndexedPartition
import scala.reflect.ClassTag
import com.vividsolutions.jts.geom.Geometry
import dbis.spark.spatial.indexed.SpatialGridPartitioner.RectRange

class IndexedSpatialPartition[G <: Geometry : ClassTag, D: ClassTag](
    private val _partitionId: Int,
    private val _bounds: RectRange,
    private val _theIndex: RTree[G,D]) extends IndexedPartition(_partitionId, _theIndex) {
  
  protected[spatial] def bounds = _bounds
}