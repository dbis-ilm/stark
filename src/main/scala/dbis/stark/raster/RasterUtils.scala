package dbis.stark.raster

import dbis.stark.STObject.{GeoType, MBR}
import org.locationtech.jts.geom.GeometryFactory

import scala.reflect.ClassTag

object RasterUtils {
  private val geoFactory = new GeometryFactory()


  def getPixels[U : ClassTag](tile: Tile[U], geo: GeoType, isIntersects: Boolean): Tile[U] = {

    val tileGeo = tileToGeo(tile)
    val matchingTileMBR = tileGeo.intersection(geo).getEnvelopeInternal

    println(matchingTileMBR)

    val intersectionTile = mbrToTile[U](matchingTileMBR, tile.pixelWidth)

    println(tile)
    println(tile.matrix)

    println(intersectionTile)
//    println(intersectionTile.matrix)

    for(j <- 0 until intersectionTile.height) {
      for(i <- 0 until intersectionTile.width) {

        val origX = intersectionTile.ulx + tile.pixelWidth * i
        val origY = intersectionTile.uly - tile.pixelWidth * j

        
        val orig = tile.value(origX, origY)

        // TODO set only if pixel intersects/contained with original geometry
        // and not only its MBR
        intersectionTile.setArray(i, j, orig)

        // TODO: use array copy to copy rowise?
      }
    }

    intersectionTile
  }

  def tileToGeo(tile: Tile[_]): GeoType = {
    val mbr = new MBR(tile.ulx, tile.ulx + tile.width, tile.uly - tile.height, tile.uly)
    geoFactory.toGeometry(mbr)
  }

  def mbrToTile[U : ClassTag](mbr: MBR, pixelWidth: Short = 1): Tile[U] =
    new Tile[U](mbr.getMinX,mbr.getMaxY,
      math.ceil(mbr.getWidth).toInt, math.ceil(mbr.getHeight).toInt,
      pixelWidth)

}
