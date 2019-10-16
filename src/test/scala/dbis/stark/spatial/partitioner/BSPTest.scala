package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import org.scalatest.tagobjects.Slow
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class BSPTest extends FlatSpec with Matchers {

//  implicit  def arrayToHisto(array: Array[(Cell, Int)]): CellHistogram = {
//    val m = array.zipWithIndex.map{ case ((c,cnt), idx) => idx -> (c,cnt)}.toSeq
//    CellHistogram(mutable.Map(m:_*))
//  }

  private def createCells(sideLength: Double, cost: Int = 10, numXCells: Int, numYCells: Int, llStartX: Double, llStartY: Double) = {
	  
    val histo = {
      
      (0 until numYCells).flatMap { y =>
        (0 until numXCells).map { x =>
          
          val ll = NPoint(llStartX + x*sideLength, llStartY+y*sideLength)
          val ur = NPoint(ll(0) + sideLength, ll(1) + sideLength)
          
          val id = y * numXCells + x
          (Cell(id, NRectRange(ll,ur)), Random.nextInt(cost))
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
	  
    val (_,_,whole,_) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)

    val cells = GridPartitioner.getCellsIn(NRectRange(NPoint(llStartX,llStartY), NPoint(llStartX+4*sideLength,llStartY+3*sideLength)),sideLength,whole,numXCells)
    
    cells should contain only (0,1,2,3,6,7,8,9,12,13,14,15)
    
    val cells2 = GridPartitioner.getCellsIn(NRectRange(NPoint(llStartX+4*sideLength,llStartY+3*sideLength), NPoint(llStartX+6*sideLength,llStartY+6*sideLength)),sideLength,whole,numXCells)
    cells2 should contain only (22,23,28,29,34,35)
  }
  
  it should "find the correct cells in range for 4 quadratic cells" in {
    val sideLength = 4
	  val maxCost = 10
	  val numXCells = 2
	  val numYCells = 2
	  
	  val llStartX = -4
	  val llStartY = -4
	  
    val (_,_,whole,_) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)
	  
    val cells = GridPartitioner.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+2*sideLength,llStartY+1*sideLength)
        ),sideLength,whole,numXCells
      )
    
    cells should contain only (0,1)
    
    val cells2 = GridPartitioner.getCellsIn(
        NRectRange(
            NPoint(llStartX,llStartY), 
            NPoint(llStartX+1*sideLength,llStartY+2*sideLength)
        ),sideLength,whole,numXCells
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
    

    val cells = GridPartitioner.getCellsIn(NRectRange(ll,ur),sideLength,whole,numXCells)

    cells.size shouldBe histo.length

    cells.foreach { cellId =>
      noException should be thrownBy histo(cellId) 
    }
  }


  ignore should "find cells in range for strange values" in {

    val sideLength = 0.05
    val maxCost = 10
    val numXCells = 2450
    val numYCells = 180
    val llStartX = -118.0374626
    val llStartY = 33.7448744

    val (_,_,whole,_) = createCells(sideLength, maxCost, numXCells, numYCells, llStartX, llStartY)


    val cells = GridPartitioner.getCellsIn(whole,sideLength,whole,numXCells)
    cells should contain theSameElementsAs (0 until numXCells * numYCells)
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
      
    val ll = NPoint(-4,-4)
    val ur = NPoint(4,4)
    val sideLength = 4

    val start = Cell(NRectRange(ll,ur))  
    
    withClue("start") { GridPartitioner.cellsPerDimension(start.range,sideLength) should contain theSameElementsAs List(2,2) }
    withClue("cell1 +2") { GridPartitioner.cellsPerDimension(cell1.range.extend(cell2.range),sideLength) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell3 +4") { GridPartitioner.cellsPerDimension(cell3.range.extend(cell4.range),sideLength) should contain theSameElementsInOrderAs List(2,1) }
    withClue("cell1 +3") { GridPartitioner.cellsPerDimension(cell1.range.extend(cell3.range),sideLength) should contain theSameElementsInOrderAs List(1,2) }
    withClue("cell2 +4") { GridPartitioner.cellsPerDimension(cell2.range.extend(cell4.range),sideLength) should contain theSameElementsInOrderAs List(1,2) }
    
  }
  

}
