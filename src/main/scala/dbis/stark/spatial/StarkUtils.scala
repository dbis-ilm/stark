package dbis.stark.spatial

import dbis.stark.{Instant, STObject}
import org.locationtech.jts.geom._
import dbis.stark.STObject.{GeoType, MBR}

object StarkUtils {

  val MAX_LONG_INSTANT = Instant(Long.MaxValue)

  val fac = new GeometryFactory()

  @inline
  def toEnvelope(r: NRectRange): MBR = new Envelope(r.ll(0), r.ur(0), r.ll(1), r.ur(1))

  @inline
  def fromGeo(g: GeoType): NRectRange = {
    val env = g.getEnvelopeInternal
    fromEnvelope(env)
  }

  def fromEnvelope(env: MBR): NRectRange =
    NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))

  def fromEnvelope3D(env: MBR): NRectRange =
    NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))

  def makeGeo(mbr: MBR): GeoType = {
    fac.toGeometry(mbr)
  }

  def convexHull(geos: Iterable[STObject]): STObject = {
    val hull = fac.createGeometryCollection(geos.map(_.getGeo).toArray).convexHull()

    STObject(hull)

  }


  def getCenter(g: GeoType): Point = {
    val center = g.getCentroid

//    if(center.getX.isNaN || center.getY.isNaN) {
//      val distincts = g.getCoordinates.distinct
//
//      if(distincts.length == 1) {
//        center = GeometryFactory.createPointFromInternalCoord(distincts.head, g)
//      } else {
//        println(s"distincts: ${distincts.mkString(";")}")
//      }
//    }
    center
  }

  def createPoint(x: Double, y:Double): Point = {
    fac.createPoint(new Coordinate(x,y))
  }
}
