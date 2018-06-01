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
    println(intersectionTile.matrix)

    var y = intersectionTile.height
    while(y >= 0) {
      for(x <- 0 until intersectionTile.width) {

//        val origX = math.ceil(intersectionTile.ulx - x).toInt
//        val origY = math.ceil(intersectionTile.uly - y).toInt

//        println(intersectionTile.ulx + x - tile.ulx)

        val origX = intersectionTile.ulx.toInt + tile.pixelWidth * x
        val origY = intersectionTile.uly.toInt + tile.pixelWidth * y

        print(s"$x,$y ==> $origX, $origY : ")

        try {
          val orig = tile.value(origX, origY)

          println(orig)
          intersectionTile.setArray(x, y, orig)
        } catch {
          case e: ArrayIndexOutOfBoundsException => println(s"error : ${e.getMessage}")
        }
      }
      y -= 1
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
