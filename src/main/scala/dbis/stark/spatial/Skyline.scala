package dbis.stark.spatial

import dbis.stark.STObject

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

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

  def euclidDist(l: STObject, r: STObject): (Double, Double) =
    (l.getGeo.distance(r.getGeo), l.getTemp.get.center.get - r.getTemp.get.center.get)
}

class Skyline[V : ClassTag](val skylinePoints: ListBuffer[(STObject, (STObject, V))]) extends Serializable {
  type P = (STObject, V)
  type T = (STObject, P)

//  private[spatial] var skylinePoints = ListBuffer.empty[T]

  def isEmpty = skylinePoints.isEmpty

  def reset() = skylinePoints.clear()

  def insert(tuple: T): Skyline[V] = {
    if(!skylinePoints.exists{ case (sp,_) => Skyline.centroidDominates(sp, tuple._1)}) {

      val l = skylinePoints.filter{ case (sp,_) => Skyline.centroidDominates(tuple._1, sp)}

      l += tuple

      new Skyline[V](l)
    }

    new Skyline(skylinePoints)
  }
}
