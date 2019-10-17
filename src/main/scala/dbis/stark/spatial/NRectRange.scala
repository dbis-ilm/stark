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
 * @param ll The lower left point (min value of each dimension)
 * @param ur The upper right point (max value in each dimension)
 */
case class NRectRange(ll: NPoint, ur: NPoint) extends Cloneable with WKT {

  def dist(p: NPoint) = {
    points.iterator.map { corner =>
      math.sqrt(corner.c.iterator.zip(p.c.iterator).map{ case (l,r) => l - r}.map(math.pow(_,2)).sum)
    }.min
  }


  require(ll.dim >= 2, "dimension must be >= 2")
	require(ll.dim == ur.dim, "ll and ur points must be of same dimension")
//  require(ll.c.zipWithIndex.forall { case (e,i) => e < ur(i) }, s"ll must be smaller than ur (ll: $ll  ur: $ur)")
  
  /**
   * Check if this range contains a given point.
   * 
   * @param p The point to check
   */
  def contains(p: NPoint): Boolean = {
    // right open interval p must be "smaller" than the max values of our range
    var idx = 0
    var res = true
    while(idx < p.c.length) {
      res &&= p.c(idx) >= ll(idx) && p.c(idx) < ur(idx)
      if(!res)
        return false

      idx += 1
    }
    res

//    p.c.zipWithIndex.forall { case (e, idx) => e >= ll(idx) && e < ur(idx) }
  }
  
  /**
   * Check if the this range completely contains another NRectRange 
   * 
   * @param r The other range to check for containment
   */
  def contains(r: NRectRange): Boolean = {
//    r.ll.c.zipWithIndex.forall { case (e, idx) => e >= ll(idx) } && r.ur.c.zipWithIndex.forall { case (e, idx) => e <= ur(idx) }

    var idx = 0
    var res = true
    while(idx < r.ll.c.length && res) {
      res &&= r.ll.c(idx) >= ll(idx) && r.ur.c(idx) <= ur(idx)
//      if(!res)
//        return false

      idx += 1
    }
    res
  }

  /**
    * Tests if the two ranges intersect by checking if one range contains at
    * least one corner point of the other one
    *
    * WARN: Currently, there is a special case that is not treated, because it was not needed
    *
    *      +----+
    *      |    |
    *  +-------------+
    *  |             |
    *  +-------------+
    *     |    |
    *    +----+
    *
    *
    * @param r The other range to check for intersection with
    * @return Returns true if both intersect
    */
  def intersects(r: NRectRange): Boolean = /*this.contains(r) || r.contains(this) ||*/
    points.exists { p => r.contains(p) } || r.points.exists(p => this.contains(p))

  //TODO: make n-dimensional
  lazy val points = Array(ll, NPoint(ur(0),ll(1)),ur, NPoint(ll(0),ur(1)))
  
  override def wkt: String = s"POLYGON(( ${(points :+ ll).map(p => s"${p(0)} ${p(1)}").mkString(", ")} ))"

  /**
    * Extent this range with the given other range. That is, the minimum and maximum value in each dimension
    * of the two rangess
    * @param other The other range
    * @return The combined extent.
    */
  def extend(other: NRectRange) = NRectRange(
      this.ll.mergeMin(other.ll).mergeMin(other.ur),
      this.ur.mergeMax(other.ur).mergeMax(other.ll)
    )

  def extend(p: NPoint, eps: Double = 0) = NRectRange(
    this.ll.mergeMin(p),
    this.ur.mergeMax(if(eps <= 0) p else NPoint(p.c.map(_ + eps)))
  )
  
  def diff(other: NRectRange): NRectRange = {
    
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
  
  lazy val center = NPoint(Array.tabulate(dim){ i => ll(i) + (lengths(i) / 2) })
    
  /** 
   *  Dimensionality of the geometry
   */
  def dim: Int = ll.dim
  
  /**
   * The side lengths of the geometry. Lazily evaluated.
   */
  lazy val lengths: Array[Double] = (0 until dim).map { i => math.abs(ur(i) - ll(i)) }.toArray
  
  /**
   * The volume (area, in 2D) of the geometry. Lazily evaluated
   */
  lazy val volume: Double = lengths.product

  /**
    * Compare two [[NRectRange]]s - but only inexact to account for rounding errors in Double
    *
    * See [[NPoint.~=]]
    * @param r The other range
    * @return True when almost equal. Otherwise false
    */
  def ~=(r: NRectRange): Boolean =
    (ll ~= r.ll) && (ur ~= r.ur)


  /**
   * Check if this NRechtRange is equal to some other object.
   * <br><br>
   * They can only be equal if the other object is also a NRectRange 
   * which has the same <code>ll</code> and <code>ur</code> values.
   * The ID is <emph>NOT</emph> considered for equality check!  
   */
  override def equals(that: Any): Boolean = that match {
    case NRectRange(l, u) => (this.ll equals l) && (this.ur equals u)
    case _ => false
  }
  
  override def hashCode(): Int = (ll,ur).hashCode() // hashcode of a pair of points ll & ur
  
  override def clone(): NRectRange = NRectRange(ll.clone(), ur.clone())
}