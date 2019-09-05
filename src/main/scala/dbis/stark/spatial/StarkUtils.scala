package dbis.stark.spatial

import dbis.stark.STObject
import org.locationtech.jts.geom._
import dbis.stark.STObject.{GeoType, MBR}

object StarkUtils {

  @inline
  def toEnvelope(r: NRectRange): MBR = new Envelope(r.ll(0), r.ur(0), r.ll(1), r.ur(1))

  @inline
  def fromGeo(g: GeoType): NRectRange = {
    val env = g.getEnvelopeInternal
    fromEnvelope(env)
  }

  def fromEnvelope(env: MBR): NRectRange =
    NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))


  def makeGeo(mbr: MBR): GeoType = {
    val fac = new GeometryFactory()
    fac.toGeometry(mbr)
  }

  def convexHull(geos: Iterable[STObject]): STObject = {

    val factory = new GeometryFactory()
    val hull = factory.createGeometryCollection(geos.map(_.getGeo).toArray).convexHull()

    STObject(hull)

  }


  def getCenter(g: GeoType): Point = {
    var center = g.getCentroid

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
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPoint(new Coordinate(x,y))
  }
}
