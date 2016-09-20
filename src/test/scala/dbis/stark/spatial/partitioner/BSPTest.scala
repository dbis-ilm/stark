package dbis.stark.spatial.partitioner

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.Cell
import dbis.stark.spatial.NRectRange

class BSPTest extends FlatSpec with Matchers {
  
  "A BSP" should "find correct cells in range" in {
    
	  val sideLength = 3
	  val maxCost = 10
	  val numXCells = 6
	  val numYCells = 6
	  
	  val llStartX = -18
	  val llStartY = -11
	  
    val histo = {
      
      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>
          
          val ll = NPoint(llStartX + x*sideLength, llStartY+y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)
          
          val id = y * numXCells + x
          (Cell(NRectRange(id, ll,ur)), 10)
        }
      }.toArray
      
    }
	  
	  val ll = NPoint(llStartX, llStartY)
	  val ur = NPoint(llStartX+numXCells*sideLength+1, llStartY+numYCells*sideLength+1) 
	  val whole = NRectRange(ll, ur)
	  
    val bsp = new BSP(
      ll,
      ur,
      histo,
      sideLength,
      maxCost      
    )
    
    val cells = bsp.getCellsIn(NRectRange(NPoint(llStartX,llStartY), NPoint(llStartX+4*sideLength,llStartY+3*sideLength)), llStartX, llStartY)
    
    cells should contain only (0,1,2,3,6,7,8,9,12,13,14,15)
    
    val cells2 = bsp.getCellsIn(NRectRange(NPoint(llStartX+4*sideLength,llStartY+3*sideLength), NPoint(llStartX+6*sideLength,llStartY+6*sideLength)), llStartX, llStartY)
    cells2 should contain only (22,23,28,29,34,35)
  }
}