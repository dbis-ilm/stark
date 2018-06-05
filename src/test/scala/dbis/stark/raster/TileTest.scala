package dbis.stark.raster

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.matchers.BePropertyMatcher

class TileTest extends FlatSpec with Matchers {

  "A tile" should "be created with correct parameters" in {
    val tile = new Tile(10, 5, Array.fill(50)(0))
    tile.value(2, 2) shouldEqual 0
    tile.width shouldEqual 10
    tile.height shouldEqual 5
  }

  it should "make correct access for ulx uly" in {
    val width = 16
    val height = 11
    val tile = Tile(0, height, width, height, Array.tabulate(width*height)(identity))

    tile.value(0, 11) shouldBe 0
  }

  it should "make correct access for width height" in {
    val width = 16
    val height = 11
    val tile = Tile(0, height, width, height, Array.tabulate(width*height)(identity))

    tile.value(width - 1, 1) shouldBe width * height - 1
  }

  it should "make correct access for position within" in {
    val width = 16
    val height = 11
    val tile = Tile(0, height, width, height, Array.tabulate(width*height)(identity))

    // max value in array is with * height - 1
    // now the cell to lookup is one before the last one
    tile.value(14.5, 0.5) shouldBe width * height - 2

    tile.value(4.5, 6.5) shouldBe 68
  }

  it should "be updatable" in {
    val tile = new Tile(10, 5, Array.fill(50)(0))
    tile.set(2, 2, 4)
    tile.value(2, 2) shouldEqual 4
  }

  it should "be created from another tile using map" in {
    val tile = new Tile(10, 5, Array.fill(50)(0))
    val other = tile.map(_ => 10)
    other.value(0, 1) shouldEqual 10
    other.value(4, 4) shouldEqual 10
  }

  it should "print a matrix" in {
    val tile = new Tile(3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0))
    val s = tile.matrix

//    println(s)

    val ref = "0, 0, 1\n2, 1, 0\n2, 1, 0"

    s shouldBe ref
  }

  it should "compute correct position" in {
    val tile = Tile(ulx = 0, uly = 3,  3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0), pixelWidth = 1)

    tile.pos(0.5, 0.5) shouldBe 6
    tile.pos(0, 3) shouldBe 0

  }

  it should "compute correct position for bigger" in {

    val tile = new Tile(ulx = 0, uly = 11,  16, 11)

    tile.pos(4.5, 6.5) shouldBe 68
    tile.pos(14.5, 0.5) shouldBe 174

  }



  "Count" should "return the number of points with the given value" in {
    val tile = new Tile(3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0))
    tile.count(1) shouldEqual 3
    tile.count(2) shouldEqual 2
    tile.count(0) shouldEqual 4
  }


}