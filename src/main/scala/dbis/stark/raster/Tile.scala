package dbis.stark.raster

import scala.reflect.ClassTag

/**
 * Tile represents a data type for 2D raster data.
 *
 */
case class Tile[U : ClassTag](ulx: Double, uly: Double, width: Int, height: Int, data: Array[U], pixelWidth: Short = 1) extends Serializable {

  /**
   * Contructor for tile with given data.
   */
  def this(width: Int, height: Int, data: Array[U]) = this(0, height, width, height, data)

  def this(ulx: Double, uly: Double, width: Int, height: Int) =
    this(ulx, uly, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]))

  def this(ulx: Double, uly: Double, width: Int, height: Int, pixelWidth: Short) =
    this(ulx, uly, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]), pixelWidth)

  /**
    * Constructor for an empty tile of given size.
    */
  def this(width: Int, height: Int) = this(0, height, width, height)


  /**
   * Set a raster point at a given position to a value.
   */
  def set(x: Double, y: Double, v: U): Unit =
    data(pos(x,y)) = v

  def set(i: Int, v: U) = data(i) = v

  def setArray(i: Int, j: Int, v: U) = data(j * width + i) = v

  /**
   * Return the value at the given position of the raster.
   */
  def value(x: Double, y: Double): U = data(pos(x,y))

  private[raster] def pos(x: Double, y: Double): Int =
    (uly - y).toInt * width + x.toInt

  def value(i: Int): U = data(i)

  def valueArray(i: Int, j: Int): U = data(j * width + i)

  /**
   * Apply a function to each raster point and return the new resulting tile.
   */
  def map[T : ClassTag](f: U => T): Tile[T] = Tile(ulx, uly, width, height, data.map(f))

  /**
   * Count the number of points with the given value.
   */
  def count(v: U): Int = data.count(_ == v)

  /**
   * Return a string representation of the tile.
   */
  override def toString: String = s"tile(ulx = $ulx, uly = $uly, w = $width, h = $height, pixelWidth = $pixelWidth)"

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
}

//object Tile {
//  def apply(w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(w, h, data)
//  def apply(x: Double, y: Double, w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(x, y, w, h, data)
//}