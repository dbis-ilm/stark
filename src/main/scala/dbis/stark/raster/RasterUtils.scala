package dbis.stark.raster

import java.awt.image.BufferedImage

import dbis.stark.STObject.{GeoType, MBR}
import org.locationtech.jts.geom.GeometryFactory
import java.awt.Graphics2D
import java.awt.RenderingHints
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

    // make the raster tile a vector rectangle
    val tileGeo = tileToGeo(tile)
    // get the MBR of the intersection of the tile and the given geo
    val matchingTileMBR = tileGeo.intersection(geo).getEnvelopeInternal

    // convert back to tile
    val intersectionTile = mbrToTile[U](matchingTileMBR, default, tile.pixelWidth)

//    /**
//      * Helper method to apply intersection or containment operation
//      * @param pixelGeo The vector representation of a pixel
//      * @return True if the global filter matches (intersects/contains) with a pixel
//      */
//    @inline
//    /* Note, the underlying implementation of intersects and contains should do some
//     * optimizations such as MBR checks and rectangle optimizations. JTS does this.
//     */
//    def matches(pixelGeo: GeoType): Boolean = if(isIntersects) {
//      geo.intersects(pixelGeo)
//    } else {
//      geo.contains(pixelGeo)
//    }

    val matchFunc = if(isIntersects)
      geo.intersects _
    else
      geo.contains _

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
        val origValue = if(matchFunc(pixelGeo)) { //if(matches(pixelGeo)) {

          try {
            tile.value(origX, origY)
          } catch {
            case e: ArrayIndexOutOfBoundsException =>
              println(s"tile: $tile")
              println(s"i=$i j=$j  ==> x=$origX y=$origY ==> pos=${tile.idxFromPos(origX, origY)}")
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
  @inline
  def tileToGeo(tile: Tile[_]): GeoType =
    geoFactory.toGeometry(tileToMBR(tile))

  @inline
  def tileToMBR(tile: Tile[_]): MBR =
    new MBR(tile.ulx, tile.ulx + (tile.width * tile.pixelWidth), tile.uly - (tile.height * tile.pixelWidth), tile.uly)

  def mbrToTile[U : ClassTag](mbr: MBR, default: U, pixelWidth: Double = 1): Tile[U] =
    new Tile[U](mbr.getMinX,mbr.getMaxY,
      math.ceil(mbr.getWidth).toInt, math.ceil(mbr.getHeight).toInt,
      pixelWidth,
      default
    )

  def mbrToTile[U : ClassTag](mbr: MBR, computer: (Double, Double) => U, pixelWidth: Double): Tile[U] = {
    val width = (math.ceil(mbr.getWidth) / pixelWidth).toInt
    val height = (math.ceil(mbr.getHeight) / pixelWidth).toInt
    new Tile[U](mbr.getMinX,mbr.getMaxY,
      width, height,
      Array.tabulate(width*height){ idx =>

        val (i,j) = (idx % width, idx / width)
        val (posX, posY) = (mbr.getMinX + ((i % width) * pixelWidth), mbr.getMaxY - ((j / width) * pixelWidth))


        computer(posX, posY)
      },
      pixelWidth
    )
  }


  def intersects(left: Tile[_], right: Tile[_]): Boolean =
    tileToMBR(left).intersects(tileToMBR(right))

  def contains(left: Tile[_], right: Tile[_]): Boolean =
    tileToMBR(left).contains(tileToMBR(right))

  def rasterToImage[U](raster: RasterRDD[U], colorFunc: U => Int, resize: Boolean = false, imgWidth: Int = 0, imgHeight: Int = 0): BufferedImage = {
    require(!resize || (resize && imgWidth > 0 && imgHeight > 0), s"Dimensions should be > 0 resize is desired! ${this.getClass.getSimpleName}")

    val data = raster.mapPartitions(localTiles  => {
      //Collect all data of tiles in tuples of (offsetX, offsetY, data[one row of the tile])
      localTiles.map(tile => {
        val array = new Array[((Int, Int), Array[Int])](tile.height)

        for(y <- 0 until tile.height) {
          val xArray = new Array[Int](tile.width)
          for(x <- 0 until tile.width) {
            xArray(x) = colorFunc(tile.data(y * tile.width + x))
          }

          array(y) = ((tile.ulx.toInt, tile.uly.toInt - y), xArray)
        }

        array
      })
    }).reduce((t1, t2) => {
      t1 ++ t2
    })

    //Calculate MinMax-Coordinates
    var maxY = Integer.MIN_VALUE; var minY = Integer.MAX_VALUE
    var maxX = Integer.MIN_VALUE; var minX = Integer.MAX_VALUE
    data.foreach(p => {
      if(p._1._2 > maxY) maxY = p._1._2
      else if(p._1._2 < minY) minY = p._1._2

      if(p._1._1 < minX) minX = p._1._1
      else if(p._1._1 + p._2.length > maxX) maxX = p._1._1 + p._2.length
    })

    //Construct image from one-dimensional array of collected values
    val tile = Tile[Int](minX, maxY, maxX - minX, (maxY - minY) + 1, new Array[Int]((1 + maxY - minY) * (maxX - minX)))
    val img = new BufferedImage(tile.width, tile.height, BufferedImage.TYPE_INT_RGB)
    val imgRaster = img.getRaster

    data.foreach(p => {
      for(v <- p._2.indices) {
        tile.set(p._1._1 + v, p._1._2, p._2(v))
      }
    })

    imgRaster.setDataElements(0, 0, img.getWidth, img.getHeight, tile.data)

    //Resize image if desired
    if(resize) {
      val factor = Math.min(imgWidth / img.getWidth.toFloat, imgHeight / img.getHeight.toFloat)
      val w = (img.getWidth * factor).toInt
      val h = (img.getHeight * factor).toInt

      val scaled = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB)

      val g = scaled.createGraphics
      g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
      g.drawImage(img, 0, 0, w, h, null)

      scaled
    } else img
  }
}
