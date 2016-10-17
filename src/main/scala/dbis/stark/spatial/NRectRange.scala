package dbis.stark.spatial

/**
 * A class which represents a n-dimensional rectangle.
 * It can be described (in 2 D) with two points: the minimum values in 
 * each dimension and the maximum values in each dimension. 
 * In 2D this would be the lower left and upper right point.
 * <br><br>
 * <b>Note</b> This is interpreted as a right open interval, i.e. the 
 * max values do not belong to the geometry. 
 * 
 * @param id The ID of this geometry
 * @param ll The lower left point (min value of each dimension)
 * @param ur The upper right point (max value in each dimension)
 */
case class NRectRange(var id: Int, ll: NPoint, ur: NPoint) {
    
  require(ll.dim >= 2, "dimension must be >= 2")
	require(ll.dim == ur.dim, "ll and ur points must be of same dimension")
//  require(ll.c.zipWithIndex.forall { case (e,i) => e < ur(i) }, s"ll must be smaller than ur (ll: $ll  ur: $ur)")
  
  /**
   * Check if this range contains a given point.
   * 
   * @param p The point to check
   */
  def contains(p: NPoint): Boolean =
    // right open interval p must be "smaller" than the max values of our range
    p.c.zipWithIndex.forall { case(e, idx) => e >= ll(idx) && e < ur(idx) }
  
  /**
   * Check if the this range completely contains another NRectRange 
   * 
   * @param r The other range to check for containment
   */
  def contains(r: NRectRange): Boolean =
    r.ll.c.zipWithIndex.forall{case (e,idx) => e >= ll(idx)} && r.ur.c.zipWithIndex.forall { case (e, idx) => e <= ur(idx) }
  
  def intersects(r: NRectRange): Boolean = points.exists { p => r.contains(p) }
  
  def points = Array(ll, NPoint(ur(0),ll(1)),ur, NPoint(ll(0),ur(1)))
  
  def getWKTString() = s"POLYGON(( ${(points :+ ll).map(p => s"${p(0)} ${p(1)}").mkString(", ")} ))" 
  
  def extend(other: NRectRange) = NRectRange(
      this.ll.mergeMin(other.ll).mergeMin(other.ur),
      this.ur.mergeMax(other.ur).mergeMax(other.ll)
    )
  
  def diff(other: NRectRange) = {
    
    // FIXME: arbitrary no. dimensions
    require(dim == 2,"works for 2 dimensions only")
    require(dim == other.dim, "both Ranges must be of same dimension")
    require(ll == other.ll, "lower left points must be equal for diff")
    require(ur(0) == other.ur(0) || ur(1) == other.ur(1), "upper x or y must be equal for diff")
    
    if(ur(0) == other.ur(0)) {
      
      val (smaller, bigger) = if(ur(1) > other.ur(1)) (other, this) else (this, other)
      NRectRange(
        NPoint(ll(0), smaller.ur(1)),
        bigger.ur.clone()
      )  
    } else {
      
  	  val (smaller, bigger) = if(ur(0) > other.ur(0)) (other, this) else (this, other)
      
      NRectRange(
        NPoint(smaller.ur(0), ll(1)),
        bigger.ur.clone()
      )
    }
    
    
    
  }  
  
  
  
    
  /** 
   *  Dimensionality of the geometry
   */
  def dim = ll.dim
  
  /**
   * The side lengths of the geometry. Lazily evaluated.
   */
  lazy val lengths = (0 until dim).map { i => math.abs(ur(i) - ll(i)) }.toArray 
  
  /**
   * The volume (area, in 2D) of the geometry. Lazily evaluated
   */
  lazy val volume = lengths.reduceLeft(_ * _)
  
  /**
   * Check if this NRechtRange is equal to some other object.
   * <br><br>
   * They can only be equal if the other object is also a NRectRange 
   * which has the same <code>ll</code> and <code>ur</code> values.
   * The ID is <emph>NOT</emph> considered for equality check!  
   */
  override def equals(that: Any): Boolean = that match {
    case NRectRange(id, l, u) => (this.ll equals l) && (this.ur equals u)
    case _ => false
  }
  
  override def hashCode() = (ll,ur).hashCode() // hashcode of a pair of points ll & ur
}

object NRectRange {
  
  def apply(ll: NPoint, ur: NPoint): NRectRange = NRectRange(-1, ll, ur)
  
}
