package org.locationtech.jts.index.strtree

import dbis.stark.StarkTestUtils.makeTimeStamp
import dbis.stark.{Distance, STObject, ScalarDistance}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions.asScalaBuffer

class RTreeTest extends FlatSpec with Matchers {
  
  def loadFile() =
    scala.io.Source.fromFile("src/test/resources/new_eventful_flat_1000.csv")
          .getLines()
          .map { line => line.split(',') }
          .map { arr => (arr(0), arr(1).toInt, arr(2), STObject(arr(7))) } 
  
  "A JTS R-Tree" should "return only candidates" in {
    
    /*
     * This just a regression test  
     */
    val tree = new STRtree(5)
    
		val reader = new WKTReader() 
    val triangle = reader.read("POLYGON(( 0 0, 10 0, 5 10, 0 0))")
    
    val triangleEnv = triangle.getEnvelopeInternal
    tree.insert(triangleEnv, (triangle,1))
    
    val queryPoint = reader.read("POINT(1 9)")
    
    val candidates = tree.query(queryPoint.getEnvelopeInternal).map(_.asInstanceOf[(Geometry, Int)])

    // the triangle does not contain the query point ...
    triangle.contains(queryPoint) shouldBe false
    // but it is returned as a result candidates (because the MBB contains the point)
    candidates should contain only ((triangle,1))
  }
  
  "The RTRee" should "contain all elements" in {
    
    val entries = loadFile().map{case (_,i,_,stobject) => (stobject,i) }.toList
          
    val tree = new RTree[(STObject, Int)](3)
    
    entries.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }
    
    tree.size() shouldBe entries.size
    val l = tree.items.toArray
    l should contain theSameElementsAs entries
  }
  
  it should "return the correct results for points" in {
    
    val entries = loadFile().toList
          
    val q: STObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  
    val tree = new RTree[(STObject, Int)](3)
    
    entries.foreach{ case (_,i,_,stobject) => tree.insert(stobject, (stobject,i)) }
    
    tree.size() shouldBe entries.size
    
    val queryresult = tree.query(q).toList
    queryresult.size shouldBe 6
    queryresult should contain only ((q,2013))
  }
  
  it should "return cancidates for point DS" in {
    
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(10 1)"),
      STObject("POINT(5 10)"),
      
      STObject("POINT(1 1)"),
      STObject("POINT(10 1)"),
      STObject("POINT(5 10)"),
      
      STObject("POINT(1 1)"),
      STObject("POINT(10 1)"),
      STObject("POINT(5 10)"),
      
      STObject("POINT(1 1)"),
      STObject("POINT(10 1)"),
      STObject("POINT(5 10)"),
      
      STObject("POINT(1 1)"),
      STObject("POINT(10 1)"),
      STObject("POINT(5 10)")
    )
    
    val tree = new RTree[(STObject, Int)](3)
    
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }
    
    val q = STObject("POLYGON((2 -1, 13 -1, 8 0, 8 2, 11 2, 5 4, 0 4, 3 2, 2 -1))")
//    println(q.getGeo.getEnvelopeInternal)
    
    val candidates = tree.query(q).toList
    
    withClue("candidates size") { candidates.size shouldBe 10 } // not just 2 because of the duplicates
    
    val result = candidates.filter(p => q.getGeo.contains(p._1))
    withClue("result size") { result.size shouldBe 0 }
    
  }

  it should "find the correct result for iterator query with one (smallest) element" in {
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(1 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 2)"),

      STObject("POINT(10 10)"),
      STObject("POINT(10 20)"),
      STObject("POINT(20 10)"),
      STObject("POINT(20 20)")
    )

    val tree = new RTree[(STObject, Int)](2)
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }

    val q = STObject("POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))")

    val res = tree.query(q).map(_._1).toList

    res should contain only STObject("POINT(1 1)")
  }

  it should "find the correct result for iterator query with one (greatest) element" in {
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(1 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 2)"),

      STObject("POINT(10 10)"),
      STObject("POINT(10 20)"),
      STObject("POINT(20 10)"),
      STObject("POINT(20 20)")
    )

    val tree = new RTree[(STObject, Int)](2)
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }

    val q = STObject("POLYGON((15 15, 25 15, 25 25, 15 25, 15 15))")

    val res = tree.query(q).map(_._1).toList

    res should contain only STObject("POINT(20 20)")
  }

  it should "find the correct empty result for non-satisfiable query" in {
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(1 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 2)"),

      STObject("POINT(10 10)"),
      STObject("POINT(10 20)"),
      STObject("POINT(20 10)"),
      STObject("POINT(20 20)")
    )

    val tree = new RTree[(STObject, Int)](2)
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }

    val q = STObject("POLYGON((30 30, 40 30, 35 35, 30 30))")

    val res = tree.query(q).map(_._1).toList

    res shouldBe 'empty
  }

  it should "find all elements" in {
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(1 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 2)"),

      STObject("POINT(10 10)"),
      STObject("POINT(10 20)"),
      STObject("POINT(20 10)"),
      STObject("POINT(20 20)")
    )

    val tree = new RTree[(STObject, Int)](2)
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }

    val q = STObject("POLYGON((0 0, 50 0, 50 50, 0 50, 0 0))")

    val res = tree.query(q).map(_._1).toList

    res should contain theSameElementsAs entries
  }

  it should "find all nearest neighbors if order is smaller than number of duplicates" in {
    val entries = Array(
      STObject("POINT(1 1)"),
      STObject("POINT(1 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 2)"),
      STObject("POINT(2 1)"),
      STObject("POINT(2 1)"),

      STObject("POINT(10 10)"),
      STObject("POINT(10 20)"),
      STObject("POINT(20 10)"),
      STObject("POINT(20 20)")
    )

    val k = 3
    val tree = new RTree[(STObject, Int)](k-1)
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }

    val q = STObject("POINT(2 1)")

    val res = tree.kNN(q,k,Distance.seuclid).toList

    res.size shouldBe k
    res.map(_._2).zipWithIndex.foreach{case (d,idx) => withClue(s"$idx"){d.asInstanceOf[ScalarDistance].value shouldBe 0.0}}
    res.map(_._1._1).foreach(_ shouldBe q)
//    res should contain theSameElementsAs entries
  }

  it should "find knn from larger DS when tree order < k" in {

    val k = 6

    val tree = new RTree[(STObject, Long)](k-2)

    val src = scala.io.Source.fromFile("src/test/resources/new_eventful_flat_1000.csv")
    src.getLines().map { line =>line.split(",") }
      .map { arr =>
        val ts = makeTimeStamp(arr(1).toInt, arr(2).toInt, arr(3).toInt)
        // we don't add the ts directly to STObject to allow tests without a temporal component
        (STObject(arr(7)), ts)
      }.foreach{ case (so, ts) => tree.insert(so, (so,ts))}
    src.close()

    tree.build()

    println(s"depth: ${tree.depth()}")
    println(s"items: ${tree.size()}")

    val q: STObject = STObject("POINT (53.483437 -2.2040706)")
    val res = tree.kNN(q,k,Distance.seuclid).toList

    res.size shouldBe k
    res.map(_._2).zipWithIndex.foreach{case (d,idx) => withClue(s"$idx"){d.asInstanceOf[ScalarDistance].value shouldBe 0.0}}
    res.map(_._1._1).foreach(_ shouldBe q)
  }
}