package dbis.stark.spatial

import org.apache.spark.Partition

class SpatialPartition(
    private val partitionId: Int, 
    val bounds: NRectRange
  ) extends Partition with Serializable {
  
  override def index = partitionId
}