package dbis.stark.raster

import scala.reflect.ClassTag

/**
 * Tile represents a data type for 2D raster data.
 *
 */
case class Tile[U : ClassTag](ulx: Double, uly: Double, width: Int, height: Int, data: Array[U]) extends Serializable {

  /**
   * Contructor for tile with given data.
   */
  def this(width: Int, height: Int, data: Array[U]) = this(0, 0, width, height, data)

  /**
   * Constructor for an empty tile of given size. 
   */
  def this(width: Int, height: Int) = this(0, 0, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]))

  /**
   * Set a raster point at a given position to a value.
   */
  def set(x: Int, y: Int, v: U): Unit = data((y - uly).toInt * width + (x - ulx).toInt) = v

  /**
   * Return the value at the given position of the raster.
   */
  def value(x: Int, y: Int): U = data((y - uly).toInt * width + (x - ulx).toInt)

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
  override def toString: String = s"tile($ulx, $uly, $width, $height)"
}

//object Tile {
//  def apply(w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(w, h, data)
//  def apply(x: Double, y: Double, w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(x, y, w, h, data)
//}