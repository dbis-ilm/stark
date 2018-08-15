package dbis.stark.spatial

import org.locationtech.jts.geom.{Envelope, GeometryFactory, Point}
import dbis.stark.STObject.{GeoType, MBR}

object Utils {

  @inline
  def toEnvelope(r: NRectRange): MBR = new Envelope(r.ll(0), r.ur(0), r.ll(1), r.ur(1))

  @inline
  def fromEnvelope(g: GeoType): NRectRange = {
    val env = g.getEnvelopeInternal
    NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))
  }

  def makeGeo(mbr: MBR): GeoType = {
    val fac = new GeometryFactory()
    fac.toGeometry(mbr)
  }


  def getCenter(g: GeoType): Point = {
    var center = g.getCentroid

    if(center.getX.isNaN || center.getY.isNaN) {
      val distincts = g.getCoordinates.distinct

      if(distincts.length == 1) {
        center = GeometryFactory.createPointFromInternalCoord(distincts.head, g)
      } else {
        println(s"distincts: ${distincts.mkString(";")}")
      }
    }

    center
  }
}
