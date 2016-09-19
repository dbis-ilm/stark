package dbis.stark.spatial.partitioner

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import dbis.stark.spatial.partitioner.BSP

class BSPTest extends FlatSpec with Matchers {
  
  "A BSP" should "find correct cells in range" in {
    
//    val histo = Array(
//      (Cell(NRectRange(0, NPoint(0,0), NPoint(5,5))),10),
//      (Cell(NRectRange(1, NPoint(5,0), NPoint(10,5))),10),
//      (Cell(NRectRange(2, NPoint(0,5), NPoint(5,10))),10),
//      (Cell(NRectRange(3, NPoint(5,5), NPoint(10,10))),10)
//    )
    
    
	  val sideLength = 3
	  val maxCost = 10
	  
    val histo = {
      val numXCells = 6
      val numYCells = 6
      
      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>
          
          val ll = NPoint(x*sideLength, y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)
          
          val id = y * numXCells + x
          (Cell(NRectRange(id, ll,ur)), 10)
        }
      }.toArray
      
    }
	  
	  println(histo.mkString("\n"))
    
    
    val bsp = new BSP(
      NPoint(0, 0),
      NPoint(10, 10),
      histo,
      sideLength,
      maxCost      
    )
    
    val cells = bsp.getCellsIn(NRectRange(NPoint(0,0), NPoint(12,9)))
    
    cells should contain only (0,1,2,3,6,7,8,9,12,13,14,15)
    
    val cells2 = bsp.getCellsIn(NRectRange(NPoint(12,9), NPoint(18,18)))
    cells2 should contain only (22,23,28,29,34,25)
  }
  
}