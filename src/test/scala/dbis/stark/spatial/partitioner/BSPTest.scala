package dbis.stark.spatial.partitioner

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.Cell
import dbis.stark.spatial.NRectRange

class BSPTest extends FlatSpec with Matchers {

  private def createCells(sideLength: Int, cost: Int = 10, numXCells: Int, numYCells: Int, llStartX: Double, llStartY: Double) = {
	  
    val histo = {
      
      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>
          
          val ll = NPoint(llStartX + x*sideLength, llStartY+y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)
          
          val id = y * numXCells + x
          (Cell(NRectRange(id, ll,ur)), cost)
        }
      }.toArray
      
    }
	  
	  val ll = NPoint(llStartX, llStartY)
	  val ur = NPoint(llStartX+numXCells*sideLength, llStartY+numYCells*sideLength) 
	  val whole = NRectRange(ll, ur)
	  
	  (ll,ur,whole, histo)
  }
  
  
  "A BSP" should "find correct cells in range" in {
    
    val sideLength = 3
	  val maxCost = 10
	  val numXCells = 6
	  val numYCells = 6
	  
	  val llStartX = -18
	  val llStartY = -11
	  
    val (ll,ur,whole,histo) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)	  
	  
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
  
  it should "find the correct cells in range for 4 quadratic cells" in {
    val sideLength = 4
	  val maxCost = 10
	  val numXCells = 2
	  val numYCells = 2
	  
	  val llStartX = -4
	  val llStartY = -4
	  
    val (ll,ur,whole,histo) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)
	  
    val bsp = new BSP(
      ll,
      ur,
      histo,
      sideLength,
      maxCost      
    )
    
    val cells = bsp.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+2*sideLength,llStartY+1*sideLength)
        ), 
        llStartX, 
        llStartY)
    
    cells should contain only (0,1)
    
    val cells2 = bsp.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+1*sideLength,llStartY+2*sideLength)
        ), 
        llStartX, 
        llStartY)
    
    cells2 should contain only (0,2)
  }
  
  it should "create correct extent information" in {
    
    val cell1 = Cell(
      range = NRectRange(0,NPoint(-4,-4), NPoint(0,0)),
      extent = NRectRange(NPoint(-7,-5), NPoint(0,1)))
    
		val cell2 = Cell(
			range = NRectRange(2,NPoint(0,-4), NPoint(4,0)),
			extent = NRectRange(NPoint(-1,-4), NPoint(3,-1)))

		val cell3 = Cell(
      range = NRectRange(1,NPoint(-4,0), NPoint(0,4)),
      extent = NRectRange(NPoint(-2,3), NPoint(2,6)))
      
    val cell4 = Cell(
      range = NRectRange(3,NPoint(0,0), NPoint(4,4)),
      extent = NRectRange(NPoint(-1,-1), NPoint(6,6)))
      
    val histo = Array(
        (cell1, 10),
        (cell2, 10),
        (cell3, 10),
        (cell4, 10))
        
    val ll = NPoint(-4,-4)
    val ur = NPoint(4,4)
    
    val bsp = new BSP(
        ll,
        ur,
        histo,
        4, //side Length
        20
      )
    
    val start = Cell(NRectRange(ll,ur))  
    val (p1,p2) = bsp.costBasedSplit(start)
    
    val expP1 = Cell(
      cell1.range.extend(cell3.range),
      NRectRange(NPoint(-7,-5), NPoint(2,6))
    )
    
    val expP2 = Cell(
      cell2.range.extend(cell4.range),
      NRectRange(NPoint(-1,-4), NPoint(6,6))
    )
    
    p1 shouldBe 'isDefined
    p2 shouldBe 'isDefined
    
    withClue("p1 range") { p1.get.range shouldBe expP1.range }
    withClue("p1 extent") { p1.get.extent shouldBe expP1.extent }
    
    withClue("p2 range") { p2.get.range shouldBe expP2.range }
    withClue("p2 extent") { p2.get.extent shouldBe expP2.extent }
    
  }
}