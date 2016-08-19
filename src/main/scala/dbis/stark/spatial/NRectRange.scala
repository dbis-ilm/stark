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
    
    require(ll.dim == ur.dim, "ll and ur points must be of same dimension")
    
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
      this.contains(r.ll) && r.ur.c.zipWithIndex.forall { case (e, idx) => e <= ur(idx) }
    
    /** 
     *  Dimensionality of the geometry
     */
    def dim = ll.dim
    
    /**
     * The side lengths of the geometry. Lazily evaluated.
     */
    lazy val lengths = (0 until dim).map { i => ur(i) - ll(i) }.toArray 
    
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
