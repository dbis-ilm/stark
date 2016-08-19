package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vector

/**
  * A point represents a n-dimensional cluster point.
  *
  * @param vec the data vector
  */
class Point(val vec: Vector) extends java.io.Serializable

/**
	* The companion object of Point.
	*/
object Point {
	def apply(v: Vector) = new Point(v)
}
