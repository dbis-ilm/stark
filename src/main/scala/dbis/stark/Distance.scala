package dbis.stark

import dbis.stark.STObject.GeoType

/**
  * This object holdes a set of distance functions
  */
object Distance {

  private def weightedDist(w1: Double, w2: Double, distFunc: (STObject, STObject) => (Double, Double))(l: STObject, r: STObject): Double = {

      val (d1,d2) = distFunc(l,r)
      w1 * d1 + w2 * d2
  }


  def seuclid(l: GeoType, r: GeoType): Double = l.getCentroid.distance(r.getCentroid)
  def seuclid(l: STObject, r: STObject): Double = seuclid(l.getGeo, r.getGeo)
  def teuclid(l: STObject, r: STObject): Double = l.getTemp.get.center.get - r.getTemp.get.center.get

  def euclid(l: STObject, r: STObject): (Double, Double) = (seuclid(l,r), teuclid(l,r))

  def euclid(sWeight: Double, tWeight: Double)(l: STObject, r: STObject): Double = weightedDist(sWeight, tWeight, euclid)(l,r)



  /////////////////////////////////////////////////////////////////////////////////

  def shausdorff(l: GeoType, r: GeoType): Double = {

    val lFactory = l.getFactory
    val leftBorderPoints = l.getBoundary.getCoordinates.map(lFactory.createPoint)

    val rFactory = r.getFactory
    val rightBorderPoints = r.getBoundary.getCoordinates.map(rFactory.createPoint)

    val dist = leftBorderPoints.map{ lp =>
      rightBorderPoints.map{ rp => seuclid(lp,rp) }.min
    }.max


    dist
  }

  def thausdorff(l: TemporalExpression, r: TemporalExpression): Double = {
    val lEnd = l.end.getOrElse(l.start)
    val rEnd = r.end.getOrElse(r.start)

    math.max(math.abs(l.start - r.start), math.abs(lEnd - rEnd))
  }

  def hausdorff(l: STObject, r: STObject): (Double, Double) =
    (shausdorff(l.getGeo, r.getGeo), thausdorff(l.getTemp.get, r.getTemp.get))


  def hausdorff(sWeight: Double, tWeight: Double)(l: STObject, r: STObject): Double = weightedDist(sWeight, tWeight, hausdorff)(l,r)

}
