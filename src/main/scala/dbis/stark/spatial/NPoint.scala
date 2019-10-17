package dbis.stark.spatial

trait WKT {
  def wkt: String
}

/**
 * A class representing a point in a n-dimensional space
 * 
 * @param c: The array of coordinate values in each dimension
 */
case class NPoint(c: Array[Double]) extends Cloneable with WKT {

  require(!c.exists(_.isNaN), s"Coordinate value must not be NaN: ${c.mkString("; ")}")
  require(c.length >= 2, "dimension must be >= 2")

  override def wkt: String = s"POINT(${c.mkString(" ")})"

  def apply(idx:Int): Double = c(idx)

  /**
    * The dimensionality of this point
    */
  def dim: Int = c.length


  //	def mergeMin(other: NPoint) = NPoint(
  //	    c.zip(other.c).map { case (l,r) => math.min(l, r) }
  //    )

  //  def mergeMax(other: NPoint) = NPoint(
  //    c.zip(other.c).map { case (l,r) => math.max(l, r) }
  //  )

  def mergeMin(other: NPoint): NPoint = {
    val arr = new Array[Double](c.length)
    var i = 0
    while(i < c.length) {
      arr(i) = math.min(c(i), other.c(i))
      i += 1
    }
    NPoint(arr)
  }

  def mergeMax(other: NPoint): NPoint = {
    val arr = new Array[Double](c.length)
    var i = 0
    while(i < c.length) {
      arr(i) = math.max(c(i), other.c(i))
      i += 1
    }
    NPoint(arr)
  }

  def withValue(dim: Int, newValue: Double): NPoint = {
    val newC = c.clone()
    newC.update(dim, newValue)
    NPoint(newC)
  }

  /**
    * Compare two [[NPoint]]s inexactly.
    * We round each coordinate value to [[NPoint.ROUND_SCALE]] decimal places and compare them
    * @param o The other point
    * @return True if all coordinate values are equal up to [[NPoint.ROUND_SCALE]] decimals. Otherwise false
    */
  def ~=(o: NPoint): Boolean =
    c.zip(o.c).forall{ case (l,r) =>
      val lbd = BigDecimal(l).setScale(NPoint.ROUND_SCALE, BigDecimal.RoundingMode.HALF_UP)
      val rbd = BigDecimal(r).setScale(NPoint.ROUND_SCALE, BigDecimal.RoundingMode.HALF_UP)
      lbd.equals(rbd)
    }

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
	override def hashCode(): Int = this.c.deep.hashCode()
	
	override def toString = s"""NPoint(${c.mkString(" ")})"""
	
  override def clone(): NPoint = NPoint(this.c.clone())
}

object NPoint {

  var ROUND_SCALE = 5

	def apply(x: Double, y: Double): NPoint = {
    require(x != Double.NaN, "x value must not be NaN")
    require(y != Double.NaN, "y value must not be NaN")
    NPoint(Array(x,y))
  }
	
}
