package dbis.stark.spatial

/**
 * This class represents a cell or Partition
 * 
 * @param range The computed bounds of the cell
 * @param extent The theoretical bounds of the cell with the minimum and maximum extent of the contained geometries
 */
case class Cell(range: NRectRange, extent: NRectRange) {
  override def hashCode() = range.hashCode()
  override def equals(other: Any) = other match {
    case Cell(otherRange, _) => range.equals(otherRange)
    case _ => false
  }
}

object Cell {
  
  def apply(r: NRectRange): Cell = Cell(r,r)
}