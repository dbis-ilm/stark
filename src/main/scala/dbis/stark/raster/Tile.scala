package dbis.stark.raster

import scala.reflect.{ClassTag, _}

case class SMA[@specialized(Int, Double, Byte) U : ClassTag](var min: U,
                                                             var max: U,
                                                             var median: U) {
  override def toString = s"$min,$max,$median"
}

/**
 * Tile represents a data type for 2D raster data.
 *
 */
  case class Tile[U : ClassTag](ulx: Double, uly: Double,
                                width: Int, height: Int,
                                data: Array[U], pixelWidth: Double = 1,
                                var sma: Option[SMA[U]] = None)(implicit ord: Ordering[U]) {

  /**
   * Contructor for tile with given data.
   */
//  def this(width: Int, height: Int, data: Array[U]) = this(0, height, width, height, data)
//
//  def this(ulx: Double, uly: Double, width: Int, height: Int) =
//    this(ulx, uly, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]))
//
//  def this(ulx: Double, uly: Double, width: Int, height: Int, pixelWidth: Double, default: U) =
//    this(ulx, uly, width, height, Array.fill[U](width * height)(default), pixelWidth)

  /**
    * Constructor for an empty tile of given size.
    */
//  def this(width: Int, height: Int) = this(0, height, width, height, Array.fill[U](width * height)(null.asInstanceOf[U]))

  def computeSMA(): Tile[U] = {
    if(data.isEmpty)
      return this

    var min = data(0)
    var max = data(0)

    //TODO: compute average -- need sum function

    var i = 1
    while(i < data.length){
      if(ord.compare(data(i), min) < 0)
        min = data(i)
      else if(ord.compare(data(i),max) > 0)
        max = data(i)

      i += 1
    }

    val median = data(data.length / 2)

    this.sma = Some(SMA(min, max, median))
    this
  }

  def updateSMA(u: U): Unit = sma match {
    case Some(theSMA) =>
      if(ord.compare(u,theSMA.min) < 0)
        theSMA.min = u
      else if (ord.compare(u,theSMA.max) > 0)
        theSMA.max = u

      // TODO: update average
    case None => //computeSMA()
  }

  //Used in RasterUtils.calcByteRasterMinMax
  def getSMA = sma

  lazy val center = (ulx + (width*pixelWidth)/2 , uly - (height*pixelWidth)/2)

  /**
   * Set a raster point at a given position to a value.
   */
  def set(x: Double, y: Double, v: U): Unit = {
    data(idxFromPos(x, y)) = v
    updateSMA(v)
  }

  def set(i: Int, v: U) = {
    data(i) = v
    updateSMA(v)
  }

  def setArray(i: Int, j: Int, v: U) = {
    data(j * width + i) = v
    updateSMA(v)
  }

  def setRow(x: Double, y: Double, array: Array[U]) = {
    val r = row(y)
    val c = column(x)
    setRowIdx(c,r, array)
  }

  def setRowIdx(i: Int, j: Int, array: Array[U]) = {
//    require(array.length == width, s"array length (${array.length} must match tile width ${width}")
    val start = j * width + i
    Array.copy(array, 0, data, start, array.length)
  }

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

  def valueArray(i: Int, j: Int): U = {
    val idx = j * width + i
    require(idx < data.length, s"provided index ($idx) must be < ${data.length}")
    data(idx)
  }

  /**
   * Apply a function to each raster point and return the new resulting tile.
   */
  def map[T : ClassTag](f: U => T)(implicit ord: Ordering[T]): Tile[T] = {
    val t = Tile(ulx, uly, width, height, data.map(f),pixelWidth)
    t.computeSMA()
    t
  }

  /**
   * Count the number of points with the given value.
   */
  def countValue(v: U): Int = accessorHelper(v, 0)(_.count(_ == v))


  def hasValue(v: U): Boolean = accessorHelper(v,false)(_.contains(v))

  def hasAllValues(min: U, max: U, vs: Seq[U]): Boolean = sma match {
    case Some(SMA(minSMA, maxSMA,_)) =>
      if(ord.compare(min, minSMA) >= 0 && ord.compare(max,maxSMA) <= 0)
        vs.forall{v => data.contains(v)}
      else
        false
    case None => vs.forall{v => data.contains(v)}
  }


  private def accessorHelper[R](v: U, default: R)(f: Array[U] => R):R = sma match {
    case Some(SMA(min, max,_)) =>
      if(ord.compare(v, min) >= 0 && ord.compare(v,max) <= 0)
        f(data)
      else
        default
    case None => f(data)
  }

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

  /**
    * Calculates the histogram for this tile
    * Notice: A very efficient version (more than twice as fast) of this calculation can be found in RasterUtils.createByteHistogram
    * which takes the sma's min/max into effect
    */
  def histogram(buckets: Int = 0, minMax: (U, U) = null): Seq[Bucket[U]] = {
    require(buckets >= 0, s"Bucket count can't be less than 0 in ${this.getClass.getSimpleName}!")

    if(data.length == 0) return Nil

    //Try to use fast option
    if(data(0).isInstanceOf[Byte]) {
      if(minMax != null) return RasterUtils.createByteHistogram(this.asInstanceOf[Tile[Byte]], buckets, minMax.asInstanceOf[(Byte, Byte)]).asInstanceOf[Seq[Bucket[U]]]
      else sma match {
        case Some(SMA(min, max,_)) =>
          return RasterUtils.createByteHistogram(this.asInstanceOf[Tile[Byte]], buckets, (min.asInstanceOf[Byte], max.asInstanceOf[Byte])).asInstanceOf[Seq[Bucket[U]]]
        case None =>
      }
    }

    //Use generic slower version instead

    var res = Nil : Seq[Bucket[U]]
    val seq = data.groupBy(identity).mapValues(_.length).toSeq.sortBy(_._1)
    val stepSize = if(buckets != 0) Math.ceil(seq.size.toDouble / buckets.toDouble).toInt else 1
    val upperBound = if(buckets != 0) buckets else seq.length

    def id(i: Int) = Math.min(i, seq.size - 1)

    for(i <- 0 until upperBound - 1) {
      var c = 0
      val lower = seq(i * stepSize)._1
      val upper = seq(id((i + 1) * stepSize - 1))._1

      for(j <- (i * stepSize) to  id((i+1) * stepSize - 1)) {
        c += seq(j)._2
      }

      res = res :+ Bucket(c, lower, upper)
    }

    res
  }
}

object Tile {
//  def apply(w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(w, h, data)
//  def apply(x: Double, y: Double, w: Int, h: Int, data: Array[Byte]) : Tile = new Tile(x, y, w, h, data)

  def apply[T:ClassTag](ulx: Double, uly: Double, width: Int, height: Int)(implicit ord: Ordering[T]): Tile[T] =
    Tile(ulx,uly,width, height, Array.fill(width*height)(null.asInstanceOf[T]))
  def apply[T:ClassTag](width: Int, height: Int, arr: Array[T])(implicit ord: Ordering[T]): Tile[T] =
    Tile(0,height, width, height, arr)
}