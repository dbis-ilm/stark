package dbis.stark.spatial.partitioner

import java.nio.file.{Path, Paths}

import scala.collection.JavaConverters._
import dbis.stark.STObject
import dbis.stark.spatial.{Cell, NPoint, NRectRange, Utils}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Contains convenience functions used in spatial partitioners
  */
object SpatialPartitioner {

  var EPS: Double = 1 / 1000000.0


  /**
    * Determine the min/max extents of a given RDD
    *
    * Since we use right-open intervals in [[NRectRange]] we add 1 to the both max values
    *
    * @param rdd The RDD
    * @tparam G The data type representing spatio-temporal data
    * @tparam V The type for payload data
    * @return Returns a 4-tuple for min/max values in the two dimensions in the form <code>(min-x, max-x, min-y, max-y)</code>
    */
  protected[stark] def getMinMax[G <: STObject, V](rdd: RDD[(G,V)]): (Double, Double, Double, Double) = {
    val (minX, maxX, minY, maxY) = rdd.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
      
    }.reduce { (oldMM, newMM) => 
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4
      
      (newMinX, newMaxX, newMinY, newMaxY)  
    }
    
    // do +1 for the max values to achieve right open intervals 
    (minX, maxX + EPS, minY, maxY + EPS)
  }


  protected[stark] def getCellId(_x: Double, _y: Double, minX: Double, minY: Double, maxX: Double, maxY: Double, xLength: Double, yLength:Double, numXCells: Int): Int = {
    require(_x >= minX && _x <= maxX && _y >= minY && _y <= maxY, s"(${_x},${_y}) out of range!")

    val x = math.floor(math.abs(_x - minX) / xLength).toInt
    val y = math.floor(math.abs(_y - minY) / yLength).toInt

    val cellId = y * numXCells + x

    cellId
  }

  /**
    * Compute the bounds of a cell with the given ID
    * @param id The ID of the cell to compute the bounds for
    */
  protected[spatial] def getCellBounds(id: Int, xCells: Int, xLength: Double, yLength: Double, minX: Double, minY: Double): NRectRange = {

    val dy = id / xCells
    val dx = id % xCells

    val llx = dx * xLength + minX
    val lly = dy * yLength + minY

    val urx = llx + xLength
    val ury = lly + yLength

    NRectRange(NPoint(llx, lly), NPoint(urx, ury))
  }


  def buildHistogram[G <: STObject, V](rdd: RDD[(G,V)], pointsOnly: Boolean, numXCells: Int, numYCells: Int, minX: Double, minY: Double, maxX: Double, maxY: Double, xLength: Double, yLength:Double): Array[(Cell,Int)] = {

    val histo = buildGrid(numXCells,numYCells, xLength, yLength, minX,minY)

    /* fill the array. If with extent, we need to keep the exent of each element and combine it later
     * to create the extent of a cell based on the extents of its contained objects
     */
    if(pointsOnly) {
      rdd.map{ case (g,_) =>
        val p = Utils.getCenter(g.getGeo)

        val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)

        (cellId, 1)
      }
      .reduceByKey(_ + _)
//      .collect
      .cache()
      .toLocalIterator
      .foreach{ case (cellId, cnt) =>
        histo(cellId) = (histo(cellId)._1, cnt)
      }


    } else {

      rdd.map { case (g, _) =>
        val p = Utils.getCenter(g.getGeo)

        val env = g.getEnvelopeInternal
        val extent = NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))
        val cellId = getCellId(p.getX, p.getY,minX, minY, maxX, maxY, xLength, yLength, numXCells)

        (cellId,(1, extent))
      }
      .reduceByKey{ case ((lCnt, lExtent), (rCnt, rExtent)) =>
        val cnt = lCnt + rCnt

        val extent = lExtent.extend(rExtent)

        (cnt, extent)

      }
//        .collect
      .cache()
      .toLocalIterator
      .foreach{case (cellId, (cnt,ex)) =>
        histo(cellId) = (Cell(cellId, histo(cellId)._1.range, ex) , cnt)
      }
    }
    histo

  }

  def buildGrid(numXCells: Int, numYCells: Int, xLength: Double, yLength: Double, minX: Double, minY: Double): Array[(Cell, Int)] =
    Array.tabulate(numXCells * numYCells){ i =>
      val cellBounds = getCellBounds(i, numXCells, xLength, yLength, minX, minY)
      (Cell(i,cellBounds), 0)
    }

}



/**
  * Base class for spatial partitioners
  * @param minX The min value in x dimension
  * @param maxX The max value in x dimension
  * @param minY The min value in y dimension
  * @param maxY The max value in y dimension
  */
abstract class SpatialPartitioner(
    val minX: Double, var maxX: Double, val minY: Double, var maxY: Double
  ) extends Partitioner {


  def partitionBounds(idx: Int): Cell
  def partitionExtent(idx: Int): NRectRange

  def printPartitions(fName: java.nio.file.Path): Unit

  def printPartitions(fName: String): Unit = {
    printPartitions(Paths.get(fName))
  }

  protected[stark] def writeToFile(strings: List[String], fName: Path) =
    java.nio.file.Files.write(fName, strings.asJava, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
}

