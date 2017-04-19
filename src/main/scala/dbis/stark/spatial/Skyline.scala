package dbis.stark.spatial

import dbis.stark.STObject

/**
  * Created by hage on 12.04.17.
  */
object Skyline extends  Serializable{

  def centroidDominates(l: STObject, r: STObject): Boolean = {

    val sDistL = l.getGeo.getCentroid.getX
    val tDistL = l.getGeo.getCentroid.getY

    val sDistR = r.getGeo.getCentroid.getX
    val tDistR = r.getGeo.getCentroid.getY


    (tDistL < tDistR && sDistL <= sDistR) || (sDistL < sDistR && tDistL <= tDistR)
  }
}

class Skyline[PayloadType](var skylinePoints: List[(STObject, PayloadType)] = List.empty) extends Serializable {
  type T = (STObject, PayloadType)

  def insert(tuple: T): Unit = {
    if(!skylinePoints.exists{ case (sp,_) => Skyline.centroidDominates(sp, tuple._1)}) {

      this.skylinePoints = skylinePoints.filterNot{ case (sp,_) => Skyline.centroidDominates(tuple._1, sp)} ++ List(tuple)
    }
  }

  def iterator = skylinePoints.iterator
}
