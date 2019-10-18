package dbis.stark

import dbis.stark.STObject.GeoType


abstract class Distance extends Serializable {

  def <(o: Distance): Boolean
  def <=(o: Distance): Boolean
  def >(o: Distance): Boolean
  def >=(o:Distance): Boolean
  def ==(o: Distance): Boolean

  def minValue: Double
  def maxValue: Double

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: Distance => this == o
    case _ => false
  }

  override def hashCode(): Int = toString.hashCode
}


/**
  * An [[IntervalDistance]] represents a distance measure that has a minimum and a maximum value.
  *
  * @param min The smallest distance
  * @param max The greatest distance
  */
case class IntervalDistance(private val min: Double, private val max: Double) extends Distance with Serializable{

  require(min <= max, s"min value must be <= max value, but $min > $max")

  override def minValue: Double = min

  override def maxValue: Double = max

  /**
    * Test if this highest distance value is smaller than the other minimum value
    * @param o The other object to compare to
    * @return Returns true if the highest dist value is smaller than the others minimum value
    */
  override def <(o: Distance): Boolean = this.maxValue < o.minValue

  /**
    * Test if this highest value is smaller than the other max value
    * @param o The other object to compare to
    * @return Returns true if the highest value is smaller than the others highest value
    */
  override def <=(o: Distance): Boolean = this.maxValue <= o.maxValue

  /**
    * Test if this minimum distance value is greater than the other maximum value
    * @param o The other object to compare to
    * @return Returns true if the minimum dist value is greater than the others maximum value
    */
  override def >(o: Distance): Boolean = this.minValue > o.maxValue

  /**
    * Test if this minimum value is greater than the other minimum value
    * @param o The other object to compare to
    * @return Returns true if the minimum dist value is greater than the others minimum value
    */
  override def >=(o: Distance): Boolean = this.minValue >= o.minValue

  /**
    * Test if both distance measures have the same minimum and maximum value
    * @param o The other object to compare to
    * @return Returns true if the both distance values have the same minimum and maximum value
    */
  override def ==(o: Distance): Boolean = this.minValue == o.minValue && this.maxValue == o.maxValue

  override def toString: String = s"[$min;$max]"
}

//object IntervalDistance {
//  def apply(min: Double, max: Double): IntervalDistance = new IntervalDistance(min,max)
//}

/**
  * A [[ScalarDistance]] represents a distance measure with exactly one value
  *
  * @param value The distance value
  */
class ScalarDistance(val value: Double) extends IntervalDistance(value,value) with Serializable {
  override def toString: String = s"$value"

  override def equals(obj: Any): Boolean = obj match {
    case IntervalDistance(v,_) => v == value
    case v: Double => v == value
    case v: Int => v.toDouble == value
    case _ => false
  }
}

object ScalarDistance {
  def apply(value: Double): ScalarDistance = new ScalarDistance(value)
  implicit def doubleToScalarDist(v: Double): ScalarDistance = ScalarDistance(v)
}

/**
  * This object holdes a set of distance functions
  */
object Distance {

  implicit def scalarToDouble(d: ScalarDistance): Double = d.value


  implicit def distanceOrdering: Ordering[Distance] = Ordering.fromLessThan(_ < _)
  implicit def scalarDistanceOrdering: Ordering[ScalarDistance] = Ordering.fromLessThan(_ < _)
  implicit def intervalDistanceOrdering: Ordering[IntervalDistance] = Ordering.fromLessThan(_ < _)

  type STDist = (Distance, Distance)


  private def weightedDist(w1: Double, w2: Double,
                           distFunc: (STObject, STObject) => (ScalarDistance, ScalarDistance))(
                            l: STObject, r: STObject): ScalarDistance = {

      val (d1,d2) = distFunc(l,r)
      ScalarDistance(w1 * d1.value + w2 * d2.value)
  }


  def seuclid(l: GeoType, r: GeoType): ScalarDistance = l.getCentroid.distance(r.getCentroid)
  def seuclid(l: STObject, r: STObject): ScalarDistance = seuclid(l.getGeo, r.getGeo)
  def teuclid(l: STObject, r: STObject): ScalarDistance = l.getTemp.get.start - r.getTemp.get.start

  def euclid(l: STObject, r: STObject): (ScalarDistance, ScalarDistance) = (seuclid(l,r), teuclid(l,r))

  def euclid(sWeight: Double, tWeight: Double)(l: STObject, r: STObject): ScalarDistance = weightedDist(sWeight, tWeight, euclid)(l,r)



  /////////////////////////////////////////////////////////////////////////////////

  def shausdorff(l: GeoType, r: GeoType): ScalarDistance = {

    val lFactory = l.getFactory
    val leftBorderPoints = l.getBoundary.getCoordinates.map(lFactory.createPoint)

    val rFactory = r.getFactory
    val rightBorderPoints = r.getBoundary.getCoordinates.map(rFactory.createPoint)

    val dist = leftBorderPoints.map{ lp =>
      rightBorderPoints.map{ rp => seuclid(lp,rp) }.min
    }.max


    dist
  }

  def thausdorff(l: TemporalExpression, r: TemporalExpression): ScalarDistance = {
    val lEnd = l.end.getOrElse(l.start)
    val rEnd = r.end.getOrElse(r.start)

    val d = math.max(math.abs(l.start - r.start), math.abs(lEnd - rEnd))
    ScalarDistance(d)
  }

  def hausdorff(l: STObject, r: STObject): (ScalarDistance, ScalarDistance) =
    (shausdorff(l.getGeo, r.getGeo), thausdorff(l.getTemp.get, r.getTemp.get))


  def hausdorff(sWeight: Double, tWeight: Double)(l: STObject, r: STObject): ScalarDistance = weightedDist(sWeight, tWeight, hausdorff)(l,r)

}
