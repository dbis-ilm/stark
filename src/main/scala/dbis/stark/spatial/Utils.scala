package dbis.stark.spatial

import org.locationtech.jts.geom.{Envelope, GeometryFactory, Point}
import dbis.stark.STObject.{GeoType, MBR}

object Utils {
  
  def toEnvelope(r: NRectRange): MBR = new Envelope(r.ll(0), r.ur(0), r.ll(1), r.ur(1))
//  {
//    val s = s"""POLYGON ((${r.ll(0)} ${r.ll(1)}, ${r.ur(0)} ${r.ll(1)}, ${r.ur(0)} ${r.ur(1)}, ${r.ll(0)} ${r.ur(1)}, ${r.ll(0)} ${r.ll(1)}))"""
//    new WKTReader().read(s).getEnvelopeInternal
//  }

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