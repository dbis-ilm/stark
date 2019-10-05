package dbis.stark.spatial

import org.locationtech.jts.geom._

class Rectangle(val points: Array[Coordinate])
  extends Polygon(StarkUtils.fac.createLinearRing(Array(
    points(0), new Coordinate(points(1).x, points(0).y),
    points(1), new Coordinate(points(0).x, points(1).y),
    points(0))),Array.empty[LinearRing],StarkUtils.fac) {

  require(points.length == 2, s"Rectangles have only two points, got ${points.mkString(";")}")

  lazy private val coordinateSequence = factory.getCoordinateSequenceFactory.create(points)

  def this(ll: Coordinate, ur: Coordinate) =
    this(Array(ll,ur))

  def this(llx: Double, lly:Double, urx: Double, ury: Double) =
    this(Array(new Coordinate(llx, lly), new Coordinate(urx, ury)))

  override def getGeometryType: String = "rectangle"

  override def getCoordinate: Coordinate = points(0)

  override def getCoordinates: Array[Coordinate] = points

  override def getNumPoints: Int = 2

  override def isEmpty: Boolean = points.length == 0

  override def getDimension: Int = 2

  override def getBoundary: Geometry = getEnvelope

  override def getBoundaryDimension: Int = 1

  override def reverse(): Geometry = getEnvelope.reverse()

  override def equalsExact(other: Geometry, tolerance: Double): Boolean = {
    getEnvelope.equalsExact(other,tolerance)
  }

  override def copyInternal(): Rectangle = {
    val ll = new Coordinate(points(0))
    val ur = new Coordinate(points(1))
    new Rectangle(Array(ll,ur))
  }

  override def normalize(): Unit = {} // empty as it is already normalized

  override def computeEnvelopeInternal(): Envelope = new Envelope(points(0), points(1))

  override def compareToSameClass(o: Any): Int = {
    val other = o.asInstanceOf[Rectangle]
    val comparison = points(0).compareTo(other.points(0))
    if(comparison!= 0)
      comparison
    else
      points(1).compareTo(other.points(1))

  }

  override def intersects(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    getEnvelopeInternal.intersects(otherEnv)
  }

  override def contains(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    getEnvelopeInternal.contains(otherEnv)
  }

  override def covers(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    getEnvelopeInternal.covers(otherEnv)
  }

  override def overlaps(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    getEnvelopeInternal.intersects(otherEnv)
  }

  override def coveredBy(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    otherEnv.covers(getEnvelopeInternal)
  }

  override def crosses(g: Geometry): Boolean = {
    val otherEnv = g.getEnvelopeInternal
    getEnvelopeInternal.intersects(otherEnv)
  }

  override def isRectangle: Boolean = true


  private var center: Point = _
  override def getCentroid: Point = {

    if(center != null)
      return center

    val mx = points(0).x + ((points(1).x - points(0).x) / 2)
    val my = points(0).y + ((points(1).y - points(0).y) / 2)

    center = factory.createPoint(new Coordinate(mx, my))
    center

  }

  override def compareToSameClass(o: Any, comp: CoordinateSequenceComparator): Int = {
    val other = o.asInstanceOf[Rectangle]

    comp.compare(this.coordinateSequence, other.coordinateSequence)
  }

  // corresponds to Geometry.SORTINDEX_POLYGON  - which is package private
  override def getSortIndex: Int = 5
}
