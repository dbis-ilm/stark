package dbis.spark

import com.vividsolutions.jts.geom.Geometry

import SpatialObject._

/**
 * A SpatialObject represents some spatial geometry. It can also have
 * a time component, thus it represents an object in space and time.
 * 
 * @param g The geometry 
 * @param time The optional time component 
 */
case class SpatialObject(private val g: GeoType, time: Option[TemporalExpression]) extends BaseExpression[SpatialObject] {
  
  def this(g: GeoType) = this(g, None)
  
  def isValidAt(t: SpatialObject) = g.contains(t)
  
  def intersects(t: SpatialObject) = g.intersects(t)
  
  def contains(t: SpatialObject) = g.contains(t)
  
  def containedBy(t: SpatialObject) = t.contains(this)
  
  /**
     * Check if this NRechtRange is equal to some other object.
     * <br><br>
     * They can only be equal if the other object is also a NRectRange 
     * which has the same <code>ll</code> and <code>ur</code> values.
     * The ID is <emph>NOT</emph> considered for equality check!  
     */
    override def equals(that: Any): Boolean = that match {
      case SpatialObject(g, t) => (this.g equals g) && (this.time equals t)
      case _ => false
    }
    
    override def hashCode() = (g,time).hashCode()
}

object SpatialObject {
  
  type GeoType = Geometry
  
  implicit def getInternal(s: SpatialObject): GeoType = s.g
  
  implicit def makeSpatialObject(g: GeoType): SpatialObject = new SpatialObject(g)
  
}