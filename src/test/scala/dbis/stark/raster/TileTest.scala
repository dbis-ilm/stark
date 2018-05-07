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

  "A tile" should "be updatable" in {
    val tile = new Tile(10, 5, Array.fill(50)(0))
    tile.set(2, 2, 4)
    tile.value(2, 2) shouldEqual 4
  }

  "A tile" should "be created from another tile using map" in {
    val tile = new Tile(10, 5, Array.fill(50)(0))
    val other = tile.map(_ => 10)
    other.value(0, 0) shouldEqual 10
    other.value(4, 4) shouldEqual 10
  }

  "Count" should "return the number of points with the given value" in {
    val tile = new Tile(3, 3, Array(0, 0, 1, 2, 1, 0, 2, 1, 0))
    tile.count(1) shouldEqual 3
    tile.count(2) shouldEqual 2
    tile.count(0) shouldEqual 4
  }
}