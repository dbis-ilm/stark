package dbis.stark.raster

import org.scalatest.{FlatSpec, Matchers}

class TileTest extends FlatSpec with Matchers {

  "A tile" should "be created with correct parameters" in {
    val tile = new Tile(0, 5, 10, 5, Array.fill(50)(0))
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
    val tile = new Tile(0,5,10, 5, Array.fill(50)(0))
    tile.set(2, 2, 4)
    tile.value(2, 2) shouldEqual 4
  }

  it should "be created from another tile using map" in {
    val tile = new Tile(0,5,10, 5, Array.fill(50)(0))
    val other = tile.map(_ => 10)
    other.value(0, 1) shouldEqual 10
    other.value(4, 4) shouldEqual 10
  }

  it should "print a matrix" in {
    val tile = new Tile(0,3,3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0))
    val s = tile.matrix

//    println(s)

    val ref = "0, 0, 1\n2, 1, 0\n2, 1, 0"

    s shouldBe ref
  }

  it should "compute correct position" in {
    val tile = Tile(ulx = 0, uly = 3,  3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0), pixelWidth = 1)

    tile.idxFromPos(0.5, 0.5) shouldBe 6
    tile.idxFromPos(0, 3) shouldBe 0

  }

  it should "compute correct position for bigger" in {

    val tile = Tile[Int](ulx = 0, uly = 11,  16, 11)

    tile.idxFromPos(0.5, 10.5) shouldBe 0
    tile.idxFromPos(4.5, 6.5) shouldBe 68
    tile.idxFromPos(14.5, 0.5) shouldBe 174

  }

  it should "compute correct row if tile doesn't start at 0" in {
    val tile = Tile[Int](ulx = 10, uly = 10, width = 7, height = 5)

    withClue("for tile uly"){tile.row(tile.uly) shouldBe 0}
    tile.row(9.5) shouldBe 0
    tile.row(8.5) shouldBe 1
    tile.row(7.5) shouldBe 2
    tile.row(6.4) shouldBe 3
    tile.row(5.5) shouldBe 4
//    withClue("for uly - height") { tile.row(tile.uly - tile.height) shouldBe 4 }
  }

  it should "copute the correct column if tile doesn't start at 0" in {
    val tile = Tile[Int](ulx = 10, uly = 10, width = 7, height = 5)

    withClue("for tile ulx"){tile.row(tile.ulx) shouldBe 0}
    tile.column(9.5) shouldBe 0
    tile.column(8.5) shouldBe 1
    tile.column(7.5) shouldBe 2
    tile.column(6.4) shouldBe 3
    tile.column(5.5) shouldBe 4
  }

  it should "compute correct position if tile doesn't start at 0" in {

    val tile = Tile[Int](ulx = 10, uly = 10, width = 7, height = 5)

    tile.idxFromPos(12.5, 7.5) shouldBe 16
    tile.idxFromPos(16.5, 5.5) shouldBe 34
    tile.idxFromPos(10.5, 9.5) shouldBe 0

  }



  "Count" should "return the number of points with the given value" in {
    val tile = Tile(3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0))
    tile.countValue(1) shouldEqual 3
    tile.countValue(2) shouldEqual 2
    tile.countValue(0) shouldEqual 4
  }


}