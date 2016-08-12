package dbis.stark

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
  
  /*
   * The idea for the following methods is to check whether their spatial component fulfills the requirement (validity, intersection, etc)
   * and also to check their temporal components.
   * 
   * The respective check is only true iff:
   *  - the spatial check yields true AND
   *  - both temporal components are NOT defined OR
   *  - both temporal components are defined AND they also return true for the respective check 
   */
  
  /**
   * Check if this spatial object intersects with the other given object.
   * <br><br> 
   * This return <code>true</code> only iff the spatial component intersects with the given object and the 
   * temporal components are either both <b>not</b> defined, or are both defined and the this temporal
   * component intersects with the temporal expression of the given object.
   * 
   * @param t The other spatial object to check
   * @return Returns <code>true</code> iff this object intersects with the other given object in both space and time.
   */
  def intersects(t: SpatialObject) = g.intersects(t.g) && 
    (time.isEmpty && t.time.isEmpty || (time.isDefined && t.time.isDefined && time.get.intersects(t.time.get)))
  
        
  /**
   * Check if this spatial object completely contains the other given object.
   * <br><br>
   * This return <code>true</code> only iff the spatial component contains the given object and the 
   * temporal components are either both <b>not</b> defined, or are both defined and the this temporal
   * component completely contains the temporal expression of the given object.      
   * 
   * @param t The other spatial object to check
   * @return Returns <code>true</code> iff this object completely contains the other given object in both space and time
   */
  def contains(t: SpatialObject) = g.contains(t.g) && (time.isEmpty && t.time.isEmpty || (time.isDefined && t.time.isDefined && time.get.contains(t.time.get)))
  
        
  /**
   * Check if this spatial object is completely contained by the other given object.
   * <br><br>
   * This is the reverse operation of [[dbis.stark.SpatialObject#contains]] 
   */
  def containedBy(t: SpatialObject) = t.contains(this) // just the reverse operation of contains
  
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
  
  
  def getGeo = g
  def getTemp = time
  
}

object SpatialObject {
  
  def apply(g: GeoType): SpatialObject = this(g, None)
  def apply(g: GeoType, t: TemporalExpression): SpatialObject = this(g, Some(t))
  
  type GeoType = Geometry
  
  implicit def getInternal(s: SpatialObject): GeoType = s.g
  
  implicit def makeSpatialObject(g: GeoType): SpatialObject = SpatialObject(g)
  
}