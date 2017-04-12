package dbis.stark.spatial

import dbis.stark.STObject

/**
  * Created by hage on 12.04.17.
  */
object Skyline {

  def centroidDominates(l: STObject, r: STObject): Boolean = {

    val sDistL = l.getGeo.getCentroid.getX
    val tDistL = l.getGeo.getCentroid.getY

    val sDistR = r.getGeo.getCentroid.getX
    val tDistR = r.getGeo.getCentroid.getY


    (tDistL < tDistR && sDistL <= sDistR) || (sDistL < sDistR && tDistL <= tDistR)
  }

  def euclidDist(l: STObject, r: STObject): (Double, Double) =
    (l.getGeo.distance(r.getGeo), l.getTemp.get.center.get - r.getTemp.get.center.get)
}
