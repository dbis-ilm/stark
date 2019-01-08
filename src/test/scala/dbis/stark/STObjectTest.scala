package dbis.stark

import java.nio.file.{OpenOption, Paths}

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.locationtech.jts.io.{WKBWriter, WKTReader}

class STObjectTest extends FlatSpec with Matchers {
    
  "A spatial object" should "intersect with a contained polygon and interval" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
    val qryT = Interval(20L, 30L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe true }
  }

  it should "do WKB" in {
    val wkt =  "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"

    val obj = new WKTReader().read(wkt)
    val writer = new WKBWriter()


    val array = writer.write(obj)
    java.nio.file.Files.write(Paths.get("/tmp/polygon.wkb"),array)



  }

  it should "intersect with a contained polygon and instant" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
    val qryT = Instant(50L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe true }
  }
  
  it should "intersect with a contained point and instant" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POINT(7 8)")
    val qryT = Instant(89L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe true }
  }
  
  it should "intersect with itself" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val qryT = Interval(10L, 100L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe true }
  }
  
  it should "not intersect with defined vs undefined time in qry" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = Interval(10L, 100L)
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryO = STObject(qryG, None)

	  withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe false }
  }
  
  it should "not intersect with undefined time vs defined time in qry" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = None
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryT = Instant(89L)
	  val qryO = STObject(qryG, qryT)

	  withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe false }
  }
  
  it should "intersect with both undefined times" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = None
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryO = STObject(qryG, None)

	  withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe true }
  }
  
  it should "not intersect if geometry is not contained" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = Instant(89L)
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POINT( 100 100 )")
	  val qryT = Instant(89L)
	  val qryO = STObject(qryG, qryT)

	  withClue(s"$o intersects at $qryO"){ o.intersects(qryO) shouldBe false }
  }

//########################################################################################
  
  it should "correctly contain a polygon and interval" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
    val qryT = Interval(20L, 30L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe true }
  }
  
  it should "correctly contain a polygon and instant" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
    val qryT = Instant(50L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe true }
  }
  
  it should "correctly contain a point and instant" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POINT(7 8)")
    val qryT = Instant(89L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe true }
  }
  
  it should "correctly contain itself" in {
    
    val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val t = Interval(10L, 100L)
    val o = STObject(g, t)
    
    
    val qryG = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
    val qryT = Interval(10L, 100L)
    val qryO = STObject(qryG, qryT)
    
    withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe true }
  }
  
  it should "not contain with defined vs undefined time in qry" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = Interval(10L, 100L)
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryO = STObject(qryG, None)

	  withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe false }
  }
  
  it should "not contain with undefined time vs defined time in qry" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = None
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryT = Instant(89L)
	  val qryO = STObject(qryG, qryT)

	  withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe false }
  }
  
  it should "correctly contain both undefined times" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = None
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")
	  val qryO = STObject(qryG, None)

	  withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe true }
  }
  
  it should "not contain if geometry is not contained" in {

	  val g = new WKTReader().read("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	  val t = Instant(89L)
	  val o = STObject(g, t)


	  val qryG = new WKTReader().read("POINT( 100 100 )")
	  val qryT = Instant(89L)
	  val qryO = STObject(qryG, qryT)

	  withClue(s"$o contains at $qryO"){ o.contains(qryO) shouldBe false }
  }  
  
//########################################################################################
  
  it should "have the correct x y coordinates from WKT" in {
    
    val wkt = "POINT(10 20)"
    val so = STObject(wkt)
    
    so.getGeo.getCentroid.getX shouldBe 10
    so.getGeo.getCentroid.getY shouldBe 20
    
  }
}