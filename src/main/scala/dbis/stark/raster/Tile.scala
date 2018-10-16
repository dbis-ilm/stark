package dbis.stark.raster

import dbis.stark.STObject.MBR

import scala.reflect.ClassTag
import scala.reflect._

/**
 * Tile represents a data type for 2D raster data.
 *
 */
case class Tile[U : ClassTag](ulx: Double, uly: Double, width: Int, height: Int, data: Array[U], pixelWidth: Double = 1) extends Serializable {

  /**
   * Contructor for tile with given data.
   */
  def this(width: Int, height: Int, data: Array[U]) = this(0, height, width, height, data)

  def this(ulx: Double, uly: Double, width: Int, height: Int) =
    this(ulx, uly, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]))

  def this(ulx: Double, uly: Double, width: Int, height: Int, pixelWidth: Double, default: U) =
    this(ulx, uly, width, height, Array.fill[U](width * height)(default), pixelWidth)

  /**
    * Constructor for an empty tile of given size.
    */
  def this(width: Int, height: Int) = this(0, height, width, height)


  lazy val center = (ulx + (width*pixelWidth)/2 , uly - (height*pixelWidth)/2)

  /**
   * Set a raster point at a given position to a value.
   */
  def set(x: Double, y: Double, v: U): Unit =
    data(idxFromPos(x,y)) = v

  def set(i: Int, v: U) = data(i) = v

  def setArray(i: Int, j: Int, v: U) = data(j * width + i) = v

  /**
   * Return the value at the given position of the raster.
   */
  def value(x: Double, y: Double): U = data(idxFromPos(x,y))

  @inline
  private[raster] def column(x: Double): Int = math.abs(x - ulx).toInt
  @inline
  private[raster] def row(y: Double): Int = (uly - y).toInt

  @inline
  private[raster] def idxFromPos(x: Double, y: Double): Int =
    row(y) * width + column(x)


  @inline
  private[raster] def posFromColRow(i: Int, j: Int): (Double, Double) = {
    val col = ulx + ((i % width) * pixelWidth)
    val row = uly - ((j / width) * pixelWidth)

    (col, row)
  }


  @inline
  def colRow(idx: Int): (Int, Int) = {
    (idx % width, idx / width)
  }


  def value(i: Int): U = data(i)

  def valueArray(i: Int, j: Int): U = data(j * width + i)

  /**
   * Apply a function to each raster point and return the new resulting tile.
   */
  def map[T : ClassTag](f: U => T): Tile[T] = Tile(ulx, uly, width, height, data.map(f),pixelWidth)

  /**
   * Count the number of points with the given value.
   */
  def count(v: U): Int = data.count(_ == v)

  /**
   * Return a string representation of the tile.
   */
  override def toString: String = s"tile(ulx = $ulx, uly = $uly, w = $width, h = $height, pixelWidth = $pixelWidth, data = array of ${classTag[U].runtimeClass} with length ${data.length})"

  def matrix = {

    val b = new StringBuilder

    for(j <- 0 until height) {
      for(i <- 0 until width) {
        b.append(valueArray(i,j))

        if(i == width - 1) {
          if(j < height - 1)
            b.append("\n")
        } else
          b.append(", ")
      }
    }

    b.toString()
  }

  def intersects(t: Tile[_]): Boolean = RasterUtils.intersects(this, t)

  def contains(t: Tile[_]): Boolean = RasterUtils.contains(this, t)

  lazy val wkt = RasterUtils.tileToGeo(this)
}

//object Tile {
//  def apply(w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(w, h, data)
//  def apply(x: Double, y: Double, w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(x, y, w, h, data)
//}