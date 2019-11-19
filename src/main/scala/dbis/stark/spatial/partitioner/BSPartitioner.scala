package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import dbis.stark.spatial._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object BSPartitioner {
  
  def withGridPPD[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
    _gridPPD: Double,
    _maxCostPerPartition: Double,
    withExtent: Boolean,
    _minX: Double,
    _maxX: Double,
    _minY: Double,
    _maxY: Double): BSPartitioner[G] = {
    
    val _sideLength = math.min(math.abs(_maxX - _minX), math.abs(_maxY - _minY)) / _gridPPD
    
    BSPartitioner(rdd, _sideLength, _maxCostPerPartition, withExtent, (_minX, _maxX, _minY, _maxY))
    
  }
  
  var numCellThreshold: Int = -1

  def apply[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double)):BSPartitioner[G] = {

    val theRDD = rdd.map{ case (g,_) => g.getGeo.getEnvelopeInternal }

    var (minX, maxX, minY, maxY) = minMax

    val numXCells: Int = {
      val xCells = Math.ceil(math.abs(maxX - minX) / sideLength).toInt
      maxX = minX + xCells*sideLength
      Math.ceil(math.abs(maxX - minX) / sideLength).toInt
    }

    val numYCells: Int = {
      val yCells = Math.ceil(math.abs(maxY - minY) / sideLength).toInt
      maxY = minY + yCells*sideLength
      Math.ceil(math.abs(maxY - minY) / sideLength).toInt
    }

    /**
      * The cells which contain elements and the number of elements
      *
      * We iterate over all elements in the RDD, determine to which
      * cell it belongs and then simply aggregate by cell
      */

    val partitions = {
      val histogram: CellHistogram =
        GridPartitioner.buildHistogram(theRDD,pointsOnly,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

      val start = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

      val bsp = new BSP2(start, histogram, sideLength, numXCells, maxCostPerPartition, pointsOnly, BSPartitioner.numCellThreshold)
      bsp.partitions
    }

    new BSPartitioner(partitions/*,tree*/, pointsOnly, minX, maxX, minY, maxY)
  }

  def apply[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean):BSPartitioner[G] = {
    BSPartitioner(rdd, sideLength, maxCostPerPartition, pointsOnly, GridPartitioner.getMinMax(rdd.map(_._1.getEnvelopeInternal)))
  }

}

/**
  * * A cost based binary space partitioner based on the paper
  * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
  * by He, Tan, Luo, Feng, Fan
  *
  * @param pointsOnly Regard the element's extent
  * @param _minX Minimum x value
  * @param _maxX Maximum x value
  * @param _minY Minimum y value
  * @param _maxY Maximum y value
  * @tparam G Geometry type
  */
class BSPartitioner[G <: STObject : ClassTag] private[partitioner](_partitions: Array[Cell],
                                                                  //  val indexedPartitions: RTree[Cell],
                                                                   val pointsOnly: Boolean,
                                                                   private val _minX: Double,
                                                                   private val _maxX: Double,
                                                                   private val _minY: Double,
                                                                   private val _maxY: Double) extends GridPartitioner(_partitions, _minX, _maxX, _minY, _maxY) {

  override def partitionBounds(idx: Int): Cell = partitions(idx)
  override def partitionExtent(idx: Int): NRectRange = partitions(idx).extent

  override def printPartitions(fName: java.nio.file.Path) {
    val list2 = partitions.map { cell => s"${cell.id};${cell.range.wkt};${cell.extent.wkt}" }.toList
    GridPartitioner.writeToFile(list2, fName)
  }

  override def numPartitions: Int = partitions.length

  override def getIntersectingPartitionIds(o: STObject): Array[Int] = {
    // indexedPartitions.queryL(o).map(_.id)
    val mbr = StarkUtils.fromGeo(o)
    partitions.iterator.filter(_.extent intersects mbr).map(_.id).toArray
  }

  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val c = StarkUtils.getCenter(g.getGeo)

    val pX = c.getX
    val pY = c.getY
    val pc = NPoint(pX, pY)


    // find the partitionId where the point is contained in
   val part = partitions.find(_.range.contains(pc))

    // val pIds = indexedPartitions.query(g)
    // val part = pIds.find(_.range.contains(pc))

    /*
     * If the given point was not within any partition, calculate the distances to all other partitions
     * and assign the point to the closest partition.
     * Eventually, adjust the assigned partition range and extent
     */
    val (partitionId, outside) = part.map(p => (p.id,false)).getOrElse {
//      val iter = if(partitions.length > 100) partitions.par.iterator else partitions.iterator
//      val minPartitionId12 = iter.map{ case Cell(id, range, _) => (id, range.dist(pc)) }.minBy(_._2)._1

      var minDist = Double.MaxValue
      var minPartitionId: Int = -1
      var first = true

//      println(s"num partitions: ${bsp.partitions.length}")

      for(partition <- partitions) {
        val dist = partition.range.dist(pc)
        if(first || dist < minDist) {
          minDist = dist
//          println(s"setting min partition id from $minPartitionId to ${partition.id}")
          minPartitionId = partition.id
          first = false
        }

      }

      (minPartitionId, true)
    }

//    if(outside || (!pointsOnly && sampleFraction > 0)) {
//      bsp.partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
//    }

//    if(outside) {
//      partitions(partitionId).range = partitions(partitionId).range.extend(pc)
//
//      //      if(!pointsOnly)
//      partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
//    } else if(!pointsOnly) {
//      partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
//    }

    partitionId
  }

  override def equals(obj: scala.Any) = obj match {
    case sp: BSPartitioner[G] =>
//      sp.partitions sameElements partitions
      sp.partitions.length == partitions.length && {
        partitions.forall{ case Cell(_,_,extent) =>
          sp.partitions.exists{ case Cell(_,_,otherExtent) => extent == otherExtent}
        }
      }
    case _ => false

  }
}
