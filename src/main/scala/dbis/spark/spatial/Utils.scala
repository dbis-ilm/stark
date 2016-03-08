package dbis.spark.spatial

import com.vividsolutions.jts.io.WKTReader
import dbis.spatial.NRectRange
import com.vividsolutions.jts.geom.Envelope

object Utils {
  
  def toEnvelope(r: NRectRange): Envelope = {
      val s = s"""POLYGON ((${r.ll(0)} ${r.ll(1)}, ${r.ur(0)} ${r.ll(1)}, ${r.ur(0)} ${r.ur(1)}, ${r.ll(0)} ${r.ur(1)}, ${r.ll(0)} ${r.ll(1)}))"""
      new WKTReader().read(s).getEnvelopeInternal 
    }
  
}