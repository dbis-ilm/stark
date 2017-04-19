package dbis.stark

/**
  * Created by hage on 19.04.17.
  */
object Distances {

  def seuclid(l: STObject, r: STObject): Double = l.getGeo.distance(r.getGeo)
  def teuclid(l: STObject, r: STObject): Double = l.getTemp.get.center.get - r.getTemp.get.center.get

  def euclid(l: STObject, r: STObject): (Double, Double) = (seuclid(l,r), teuclid(l,r))

  def euclid(sWeight: Double, tWeight: Double)(l: STObject, r: STObject): Double = {
    val (s,t) = euclid(l,r)
    sWeight * s + tWeight * t
  }

}
