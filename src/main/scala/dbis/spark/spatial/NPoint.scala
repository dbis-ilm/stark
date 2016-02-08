package dbis.spark.spatial

case class NPoint(c: Array[Double]) extends Cloneable {
	def apply(idx:Int) = c(idx)

	def dim = c.size

	override def equals(that: Any): Boolean = that match {
		case NPoint(vals) => this.c.deep == vals.deep
		case _ => false
	}
	
	override def toString = s"""NPoint(${c.mkString(" ")})"""
	
  override def clone(): NPoint = NPoint(this.c.clone())
}
object NPoint {
	def apply(x: Double, y: Double): NPoint = NPoint(Array(x,y))
	
}