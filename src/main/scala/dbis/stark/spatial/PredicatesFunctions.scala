package dbis.stark.spatial

import dbis.stark.STObject

object JoinPredicate extends Enumeration {
  type JoinPredicate = Value
  val INTERSECTS, CONTAINS, CONTAINEDBY = Value
  
  def predicateFunction(pred: JoinPredicate): (STObject, STObject) => Boolean = pred match {
    case INTERSECTS => PredicatesFunctions.intersects
    case CONTAINS => PredicatesFunctions.contains
    case CONTAINEDBY => PredicatesFunctions.containedby
  }
}

object PredicatesFunctions {
  
  /**
   * Returns <code>true</code> if the two given spatial objects intersect
   * 
   * @param g1 The left spatial object
   * @param g2 The right spatial object
   * @return Returns <code>true</code> if the two objects intersect, according to the definition in [[dbis.stark.STObject]],
   * otherwise <code>false</code>
   */
  def intersects(g1: STObject, g2: STObject): Boolean = g1.intersects(g2)
  
  /**
   * Returns <code>true</code> if the left object contains the right object
   * 
   * @param g1 The left spatial object
   * @param g2 The right spatial object
   * @return Returns <code>true</code> if the left object completely contains the right object, according to the definition in [[dbis.stark.STObject]],
   * otherwise <code>false</code>
   */
  def contains(g1: STObject, g2: STObject): Boolean = g1.contains(g2)
  
  /**
   * Returns <code>true</code> if the left object is contained by the right object
   * 
   * @param g1 The left spatial object
   * @param g2 The right spatial object
   * @return Returns <code>true</code> if the left object is completely contained by the right object, according to the definition in [[dbis.stark.STObject]],
   * otherwise <code>false</code>
   */
  def containedby(g1: STObject, g2: STObject): Boolean = g1.containedBy(g2)
  
  /**
   * Returns <code>true</code> if the two given objects are within a given distance, i.e.
   * 
   * <code>distFunc(g1, g2) <= maxDist</code>
   * 
   * @param g1 The left spatial object
   * @param g2 The right spatial object
   * @param maxDist The maximum distance (inclusive)
   * @param distFunc The distance function to use
   * @return Returns <code>true</code> if the two objects are within a given maximum distance, according to the definition in [[dbis.stark.STObject]],
   * otherwise <code>false</code>
   */
  def withinDistance(
      maxDist: Double, 
      distFunc: (STObject, STObject) => Double)
      (g1: STObject, g2: STObject): Boolean = distFunc(g1,g2) <= maxDist
  
}