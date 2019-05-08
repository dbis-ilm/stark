package dbis.stark.raster

import java.awt.RenderingHints
import java.awt.image.{BufferedImage, DataBufferByte}
import java.nio.file.Path

import dbis.stark.STObject.{GeoType, MBR}
import javax.imageio.ImageIO
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.GeometryFactory

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * A helper class to provide commonly used raster data functions
  */
object RasterUtils {

  // used to instantiate vector geometries
  private val geoFactory = new GeometryFactory()


  def greyScaleImgToUnsignedByteArray(bufferedImage: BufferedImage): Array[Array[Int]] = {
    val width = bufferedImage.getWidth()
    val pixels = bufferedImage.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    val result = Array.ofDim[Int](bufferedImage.getHeight(), bufferedImage.getWidth())

    var pixel = 0
    var row = 0
    var col = 0
    while ( {
      pixel < pixels.length
    }) {
      //println(pixels(pixel) + ":" + (pixels(pixel) & 0xff))
      result(row)(col) = pixels(pixel) & 0xff
      col += 1
      if (col == width) {
        col = 0
        row += 1
      }

      pixel += 1
    }

    result
  }

  /**
    * Efficient calculation of the histogram for a byte tile
    * Actually this can be adapted for every Type that is numeric
    */
  def  createByteHistogram(tile: Tile[Byte], bucketCount: Int = 0, minMax: (Byte, Byte)): Seq[Bucket[Byte]] = {
    var buckets = bucketCount
    if(buckets <= 0) buckets = minMax._2 - minMax._1 + 1

    var stepSize = Math.ceil((minMax._2.toDouble - minMax._1.toDouble) / buckets.toDouble).toInt
    val bucketArray = Array.ofDim(buckets) : Array[Int]

    tile.data.foreach {
      i =>
        if(i >= minMax._1 && i <= minMax._2) bucketArray((i - minMax._1) / stepSize) += 1
    }

    var res = Nil : Seq[Bucket[Byte]]
    for(i <- bucketArray.indices) {
      res = res :+ Bucket(bucketArray(i), Math.max(minMax._1, minMax._1 + stepSize * i).toByte, Math.min(minMax._1 + stepSize * (i + 1) - 1, minMax._2).toByte)
    }

    res
  }

  def calcByteRasterMinMax(raster: RasterRDD[Byte]): (Byte, Byte) = {
    var minMax = raster.map(
      _.getSMA match {
        case Some(SMA(min, max, _)) => (min, max)
        case None => (Byte.MaxValue, Byte.MinValue)
      }).reduce((m1: (Byte, Byte), m2: (Byte, Byte)) => {
      var min = Math.min(m1._1, m2._1).toByte
      var max = Math.max(m1._2, m2._2).toByte
      (min, max)
    })

    if(minMax._1 == Byte.MaxValue) minMax = (Byte.MinValue, Byte.MaxValue)

    minMax
  }

  //Combines the histograms of tiles to one raster histogram, this requires same order and buckets
  def combineHistograms[U](buckets: RDD[Seq[Bucket[U]]]) : Seq[Bucket[U]] = {
    buckets.reduce((f1: Seq[Bucket[U]], f2: Seq[Bucket[U]]) => {
      //histograms must be ordered in order to reduce them fast
      val res = ListBuffer[Bucket[U]]()
      for (i <- f2.indices) {
        val bucket1 = f1(i)
        res += Bucket[U](bucket1.values + f2(i).values, bucket1.lowerBucketBound, bucket1.upperBucketBound)
      }

      res
    })
  }

  //Combines two histograms
  def combineHistograms[U](buckets1: Seq[Bucket[U]], buckets2: Seq[Bucket[U]]) : Seq[Bucket[U]] = {
    val res = ListBuffer[Bucket[U]]()
    for (i <- buckets1.indices) {
      val bucket1 = buckets2(i)
      res += Bucket[U](bucket1.values + buckets1(i).values, bucket1.lowerBucketBound, bucket1.upperBucketBound)
    }

    res
  }

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
  def getPixels[U : ClassTag](tile: Tile[U], geo: GeoType, isIntersects: Boolean, default: U)(implicit ord: Ordering[U]): Tile[U] = {

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

  def mbrToTile[U : ClassTag](mbr: MBR, default: U, pixelWidth: Double = 1)(implicit ord: Ordering[U]): Tile[U] =
    new Tile[U](mbr.getMinX,mbr.getMaxY,
      math.ceil(mbr.getWidth).toInt, math.ceil(mbr.getHeight).toInt,
      Array.fill(math.ceil(mbr.getWidth).toInt * math.ceil(mbr.getHeight).toInt)(default),
      pixelWidth
    )

  def mbrToTile[U : ClassTag](mbr: MBR, computer: (Double, Double) => U, pixelWidth: Double)(implicit ord: Ordering[U]): Tile[U] = {
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

  def saveAsImage[U](path: Path, raster: RDD[Tile[U]], colorFunc: U => Int, resize: Boolean = false, imgWidth: Int = 0, imgHeight: Int = 0, compressionFactor: Float = 1.0f): Unit = {


    val img = rasterToImage(raster,colorFunc,resize,imgWidth,imgHeight, compressionFactor)

    val suffix = path.getFileName.toString.split('.')(1)
    ImageIO.write(img, suffix, path.toFile)
  }

  def rasterToImage[U](raster: RDD[Tile[U]], colorFunc: U => Int, resize: Boolean = false, imgWidth: Int = 0, imgHeight: Int = 0, compressionFactor: Float = 1.0f): BufferedImage = {
    require(!resize || (resize && imgWidth > 0 && imgHeight > 0), s"Dimensions should be > 0 resize is desired! ${this.getClass.getSimpleName}")
    require(compressionFactor <= 1.0f &&  compressionFactor > 0f, s"CompressionFactor should be in (0, 1]! ${this.getClass.getSimpleName}")

    val data = raster.mapPartitions(localTiles  => {
      //Collect all data of tiles in tuples of (offsetX, offsetY, data[one row of the tile])
      localTiles.map(tile => {
        val yStepSize = Math.ceil(tile.height.toDouble / (tile.height * compressionFactor).toInt).toInt
        val xStepSize = Math.ceil(tile.width.toDouble / (tile.width * compressionFactor).toInt).toInt
        val array = new Array[((Int, Int), Array[Int])](Math.ceil(tile.height.toDouble / yStepSize).toInt)

        for(y <- 0 until tile.height by yStepSize) {
          val xArray = new Array[Int](Math.ceil(tile.width.toDouble  / xStepSize).toInt)
          for(x <- 0 until tile.width by xStepSize) {
            val color = colorFunc(tile.valueArray(x,y))
            xArray(x / xStepSize) = color
          }

          array(y / yStepSize) = ((Math.round(tile.ulx * compressionFactor).toInt, Math.round((Math.round(tile.uly / tile.pixelWidth).toInt - y)  / yStepSize)), xArray)
        }

        array
      })
    }).reduce((t1, t2) => {
      t1 ++ t2
    })

    //Calculate MinMax-Coordinates
    val (minX, maxX, minY, maxY) = data.foldLeft((Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE)){ case ((minX1,maxX1,minY1,maxY1), ((ox,oy),arr)) =>

        val yMax = if(oy > maxY1) oy else maxY1
        val yMin = if(oy < minY1) oy else minY1

        val xMin = if(ox < minX1) ox else minX1
        val xMax = if(ox + arr.length > maxX1) ox + arr.length else maxX1

      (xMin,xMax,yMin,yMax)
    }



    //Construct image from one-dimensional array of collected values
    val tile = Tile[Int](minX, maxY, maxX - minX, (maxY - minY)+1, new Array[Int]((1 + maxY - minY) * (maxX - minX)))
    val img = new BufferedImage(tile.width, tile.height, BufferedImage.TYPE_INT_RGB)
    val imgRaster = img.getRaster


    data.foreach{ case ((ox,oy),arr) =>
        tile.setRow(ox,oy, arr)
    }

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