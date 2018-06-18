package dbis.stark.raster

import dbis.stark.STObject.{GeoType, MBR}
import org.locationtech.jts.geom.GeometryFactory

import scala.reflect.ClassTag

object RasterUtils {
  private val geoFactory = new GeometryFactory()


  def getPixels[U : ClassTag](tile: Tile[U], geo: GeoType, isIntersects: Boolean, default: U): Tile[U] = {

    val tileGeo = tileToGeo(tile)
    val matchingTileMBR = tileGeo.intersection(geo).getEnvelopeInternal

    val intersectionTile = mbrToTile[U](matchingTileMBR, default, tile.pixelWidth)

    @inline
    def matches(pixelGeo: GeoType): Boolean = if(isIntersects) {
      geo.intersects(pixelGeo)
    } else {
      geo.contains(pixelGeo)
    }

    for(j <- 0 until intersectionTile.height) {
      for(i <- 0 until intersectionTile.width) {

        val origX = intersectionTile.ulx + tile.pixelWidth * i
        val origY = intersectionTile.uly - tile.pixelWidth * j


        val pixelGeo = mbrToGeo(new MBR(origX, origX + tile.pixelWidth, origY - tile.pixelWidth, origY))
        val origValue = if(matches(pixelGeo)) {

          try {
            tile.value(origX, origY)
          } catch {
            case e: ArrayIndexOutOfBoundsException =>
              println(s"tile: $tile")
              println(s"i=$i j=$j  ==> x=$origX y=$origY ==> pos=${tile.pos(origX, origY)}")
              sys.error(e.getMessage)
          }
        } else {
          default
        }

        // TODO: use array copy to copy rowise?
        intersectionTile.setArray(i, j, origValue)

      }
    }

//    println(intersectionTile.matrix)
    intersectionTile
  }

  def mbrToGeo(mbr: MBR): GeoType = geoFactory.toGeometry(mbr)

  def tileToGeo(tile: Tile[_]): GeoType = {
    val mbr = new MBR(tile.ulx, tile.ulx + tile.width, tile.uly - tile.height, tile.uly)
    geoFactory.toGeometry(mbr)
  }

  def mbrToTile[U : ClassTag](mbr: MBR, default: U, pixelWidth: Short = 1): Tile[U] =
    new Tile[U](mbr.getMinX,mbr.getMaxY,
      math.ceil(mbr.getWidth).toInt, math.ceil(mbr.getHeight).toInt,
      pixelWidth,
      default
    )

}
