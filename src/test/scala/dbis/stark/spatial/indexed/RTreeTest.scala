package dbis.stark.spatial.indexed

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.Geometry

import scala.collection.JavaConversions.asScalaBuffer
import dbis.stark.STObject

class RTreeTest extends FlatSpec with Matchers {
  
  def loadFile() = scala.io.Source.fromFile("src/test/resources/new_eventful_flat_1000.csv")
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
          
    val tree = new RTree[STObject, (STObject, Int)](3)
    
    entries.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }
    
    tree.size() shouldBe entries.size
    val l = tree.items.map(_.data).toArray
    l should contain theSameElementsAs entries
  }
  
  it should "return the correct results for points" in {
    
    val entries = loadFile().toList
          
    val q: STObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  
    val tree = new RTree[STObject, (STObject, Int)](3)
    
    entries.foreach{ case (_,i,_,stobject) => tree.insert(stobject, (stobject,i)) }
    
    tree.size() shouldBe entries.size
    
    val queryresult = tree.query(q).toList
    queryresult.size shouldBe 6
    queryresult should contain only ((STObject(q),2013))
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
    
    val tree = new RTree[STObject, (STObject, Int)](3)
    
    entries.zipWithIndex.foreach{ case (stobject,i) => tree.insert(stobject, (stobject,i)) }
    
    val q = STObject("POLYGON((2 -1, 13 -1, 8 0, 8 2, 11 2, 5 4, 0 4, 3 2, 2 -1))")
//    println(q.getGeo.getEnvelopeInternal)
    
    val candidates = tree.query(q).toList
    
    withClue("candidates size") { candidates.size shouldBe 10 } // not just 2 because of the duplicates
    
    val result = candidates.filter(p => q.getGeo.contains(p._1))
    withClue("result size") { result.size shouldBe 0 }
    
  }
  
}