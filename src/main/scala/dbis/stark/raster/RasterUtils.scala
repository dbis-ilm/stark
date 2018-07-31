package dbis.stark.raster

import breeze.collection.mutable.SparseArray
import dbis.stark.STObject.{GeoType, MBR}
import org.locationtech.jts.geom.GeometryFactory

import scala.reflect.ClassTag

/**
  * A helper class to provide commonly used raster data functions
  */
object RasterUtils {

  // used to instantiate vector geometries
  private val geoFactory = new GeometryFactory()

  /**
    * Determine the pixels from a given tile that intersect or are completely contained in
    * the given vector geometry.
    *
    * The result will be a tile whose dimensions are determined by the MBR of the matching
    * regions. Pixels in that MBR that do not intersect with the real geometry will be assigned
    * a default value.
    * @param tile The tile to get the pixels of
    * @param geo The vector geometry to apply as a filter
    * @param isIntersects true if intersects, false for contains
    * @param default The default value to set for non-matchin pixels
    * @tparam U The pixel type
    * @return Returns a tile containing only pixels intersecting with the given geometry
    */
  def getPixels[U : ClassTag](tile: Tile[U], geo: GeoType, isIntersects: Boolean, default: U): Tile[U] = {

    org.apache.spark.mllib.linalg.SparseVector

    // make the raster tile a vector rectangle
    val tileGeo = tileToGeo(tile)
    // get the MBR of the intersection of the tile and the given geo
    val matchingTileMBR = tileGeo.intersection(geo).getEnvelopeInternal

    // convert back to tile
    val intersectionTile = mbrToTile[U](matchingTileMBR, default, tile.pixelWidth)

    /**
      * Helper method to apply intersection or containment operation
      * @param pixelGeo The vector representation of a pixel
      * @return True if the global filter matches (intersects/contains) with a pixel
      */
    @inline
    /* Note, the underlying implementation of intersects and contains should do some
     * optimizations such as MBR checks and rectangle optimizations. JTS does this.s
     */
    def matches(pixelGeo: GeoType): Boolean = if(isIntersects) {
      geo.intersects(pixelGeo)
    } else {
      geo.contains(pixelGeo)
    }

    // loop over all lines
    var j = 0
    while(j < intersectionTile.height) {

      // compute the original Y coordinate in the tile from j
      val origY = intersectionTile.uly - tile.pixelWidth * j

      // loop over all columns
      var i = 0
      while(i < intersectionTile.width) {

        // compute the original X coordinate in the tile from i
        val origX = intersectionTile.ulx + tile.pixelWidth * i

        // convert a pixel into a rectangle
        val pixelGeo = mbrToGeo(new MBR(origX, origX + tile.pixelWidth, origY - tile.pixelWidth, origY))

        /* determine the value in the original tile
         * or, if the current pixel is not within the requested filter region
         * return the default value
         */
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

        // set the value in the result tile
        // TODO: use array copy to copy rowise?
        intersectionTile.setArray(i, j, origValue)

        i += 1
      }

      j += 1
    }

    intersectionTile
  }

  /* Converts the MBR into a geometry
   * JTS does not treat MBR as geometry, that's why the conversion is needed
   */
  @inline
  def mbrToGeo(mbr: MBR): GeoType = geoFactory.toGeometry(mbr)

  /**
    * Convert the given tile into a geometry
    * @param tile The tile
    * @return The geometry representing the tile
    */
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
