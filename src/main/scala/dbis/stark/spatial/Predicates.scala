package dbis.stark.spatial

import scala.reflect.ClassTag
import dbis.stark.SpatialObject

object Predicates {
  
  def intersects[G <: SpatialObject : ClassTag, G2 <: SpatialObject : ClassTag](g1: G, g2: G2) = g1.intersects(g2)
  
  def contains[G <: SpatialObject : ClassTag, G2 <: SpatialObject : ClassTag](g1: G, g2: G2) = g1.contains(g2)
  
  def containedby[G <: SpatialObject : ClassTag, G2 <: SpatialObject : ClassTag](g1: G, g2: G2) = g2.contains(g1)
  
}