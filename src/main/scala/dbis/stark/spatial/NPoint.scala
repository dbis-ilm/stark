package dbis.stark.spatial

/**
 * A class representing a point in a n-dimensional space
 * 
 * @param c: The array of coordinate values in each dimension
 */
case class NPoint(c: Array[Double]) extends Cloneable {
  
  require(c.size >= 2, "dimension must be >= 2")
  
	def apply(idx:Int) = c(idx)

	/**
	 * The dimensionality of this point 
	 */
	def dim = c.size

	
	def mergeMin(other: NPoint) = NPoint(
	    c.zip(other.c).map { case (l,r) => math.min(l, r) }
    )
  
  def mergeMax(other: NPoint) = NPoint(
      c.zip(other.c).map { case (l,r) => math.max(l, r) }
    )
	
	/*
	 * Override this because we need to compare "deep", i.e.
	 * all values instead of just the reference, which may be 
	 * different for two arrays with the same values
	 */
	override def equals(that: Any): Boolean = that match {
		case NPoint(vals) => this.c.deep == vals.deep
		case _ => false
	}
	
	/* just as we need to override equals, we also need to 
	 * compute a deep hash code 
	 */
	override def hashCode() = this.c.deep.hashCode()
	
	override def toString = s"""NPoint(${c.mkString(" ")})"""
	
  override def clone(): NPoint = NPoint(this.c.clone())
}

object NPoint {
	def apply(x: Double, y: Double): NPoint = NPoint(Array(x,y))
	
}
