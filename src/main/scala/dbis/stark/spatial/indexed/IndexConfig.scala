package dbis.stark.spatial.indexed

object IndexType extends Enumeration {
  type IndexType = Value
  val QuadTree, RTree, IntervalTree = Value

}

import IndexType._

abstract class IndexConfig(val idxType: IndexType) extends Serializable

case class RTreeConfig(order: Int) extends IndexConfig(IndexType.RTree)
case class QuadTreeConfig(maxDepth: Int, minNum: Int) extends IndexConfig(QuadTree)
case class IntervalTreeConfig() extends IndexConfig(IntervalTree)
