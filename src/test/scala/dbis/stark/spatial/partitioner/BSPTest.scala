package dbis.stark.spatial.partitioner

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.Cell
import dbis.stark.spatial.NRectRange
import org.scalatest.tagobjects.Slow

class BSPTest extends FlatSpec with Matchers {

  private def createCells(sideLength: Double, cost: Int = 10, numXCells: Int, numYCells: Int, llStartX: Double, llStartY: Double) = {
	  
    val histo = {
      
      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>
          
          val ll = NPoint(llStartX + x*sideLength, llStartY+y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)
          
          val id = y * numXCells + x
          (Cell(id, NRectRange(ll,ur)), cost)
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
      ll.c,
      ur.c,
      histo,
      sideLength,
      maxCost      
    )
    
    val cells = bsp.getCellsIn(NRectRange(NPoint(llStartX,llStartY), NPoint(llStartX+4*sideLength,llStartY+3*sideLength)))
    
    cells should contain only (0,1,2,3,6,7,8,9,12,13,14,15)
    
    val cells2 = bsp.getCellsIn(NRectRange(NPoint(llStartX+4*sideLength,llStartY+3*sideLength), NPoint(llStartX+6*sideLength,llStartY+6*sideLength)))
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
      ll.c,
      ur.c,
      histo,
      sideLength,
      maxCost      
    )
    
    val cells = bsp.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+2*sideLength,llStartY+1*sideLength)
        )
      )
    
    cells should contain only (0,1)
    
    val cells2 = bsp.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+1*sideLength,llStartY+2*sideLength)
        )
      )
    
    cells2 should contain only (0,2)
  }
  
  
  it should "find cells in range" taggedAs Slow in {
    
    val sideLength = 1
    val maxCost = 10
    val numXCells = 360
    val numYCells = 180
    val llStartX = -180
    val llStartY = -90

    val (ll,ur,whole,histo) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)
    
    withClue("generated ll wrong") { ll shouldBe NPoint(llStartX, llStartY)  }
    withClue("generated ur wrong") { ur shouldBe NPoint(180, 90)  }
    withClue("generated histogram wrong") { histo.length shouldBe 360*180 }
    
    
    val bsp = new BSP(
      ll,
      ur,
      histo,
      sideLength,
      maxCost,
      withExtent = true
    )
    
    val cells = bsp.getCellsIn(NRectRange(ll,ur))

//    cells should contain only ((0 until histo.size):_*)
    cells.size shouldBe histo.length

    val start = System.currentTimeMillis()
    cells.foreach { cellId => 
      noException should be thrownBy histo(cellId) 
    }
    val end = System.currentTimeMillis()
  }
  
  it should "find correct cells per dimension" in {
    val cell1 = Cell(0,
      range = NRectRange(NPoint(-4,-4), NPoint(0,0)),
      extent = NRectRange(NPoint(-7,-5), NPoint(0,1)))
    
		val cell2 = Cell(2,
			range = NRectRange(NPoint(0,-4), NPoint(4,0)),
			extent = NRectRange(NPoint(-1,-4), NPoint(3,-1)))

		val cell3 = Cell(1,
      range = NRectRange(NPoint(-4,0), NPoint(0,4)),
      extent = NRectRange(NPoint(-2,3), NPoint(2,6)))
      
    val cell4 = Cell(3,
      range = NRectRange(NPoint(0,0), NPoint(4,4)),
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
        20,
        withExtent = true
      )
    
    val start = Cell(NRectRange(ll,ur))  
    
    withClue("start") { bsp.cellsPerDimension(start.range) should contain theSameElementsAs List(2,2) }
    withClue("cell1 +2") { bsp.cellsPerDimension(cell1.range.extend(cell2.range)) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell3 +4") { bsp.cellsPerDimension(cell3.range.extend(cell4.range)) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell1 +3") { bsp.cellsPerDimension(cell1.range.extend(cell3.range)) should contain theSameElementsInOrderAs List(1,2) }
    withClue("cell2 +4") { bsp.cellsPerDimension(cell2.range.extend(cell4.range)) should contain theSameElementsInOrderAs List(1,2) }
    
  }
  
  it should "create correct extent information" in {
    val cell1 = Cell(0,
      range = NRectRange(NPoint(-4,-4), NPoint(0,0)),
      extent = NRectRange(NPoint(-7,-5), NPoint(0,1)))
    
		val cell2 = Cell(2,
			range = NRectRange(NPoint(0,-4), NPoint(4,0)),
			extent = NRectRange(NPoint(-1,-4), NPoint(3,-1)))

		val cell3 = Cell(1,
      range = NRectRange(NPoint(-4,0), NPoint(0,4)),
      extent = NRectRange(NPoint(-2,3), NPoint(2,6)))
      
    val cell4 = Cell(3,
      range = NRectRange(NPoint(0,0), NPoint(4,4)),
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
        20,
        withExtent = true
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
  
  it should "process a large number of cells"taggedAs Slow  in {
    val sideLength = 0.1
    val maxCost = 10
    val numXCells = 500
    val numYCells = 500
    val llStartX = -180
    val llStartY = -90
    
    val (ll,ur,_,histo) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)
    
    val bsp = new BSP(ll,ur,histo,sideLength,100,false)
    
    bsp.partitions.length should be > 0
  }
}