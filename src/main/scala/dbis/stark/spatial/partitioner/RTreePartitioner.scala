package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.{Cell, NPoint, StarkUtils}
import org.locationtech.jts.index.strtree.STRtreePlus


object RTreePartitioner {

  def apply(samples: Array[MBR], maxCost: Int, minMax: (Double, Double, Double, Double), pointsOnly: Boolean): RTreePartitioner = {

    require(maxCost > 0)

    val dummy: Byte = 0x0

    val capacity = math.max(samples.length / maxCost, 2)
//    val tree = new RTree[Byte](capacity)
    // use STRtreePlus since it does not add geometry to payload data
    val tree = new STRtreePlus[Byte](capacity)
    var i = 0
    while(i < samples.length) {
      val g = samples(i)
      tree.insert(g, dummy)
      i += 1
    }
    tree.build()

    val children = tree.queryBoundary() // is a Java ArrayList!

    val partitions = new Array[Cell](children.size())
    i = 0
    while(i < children.size()) {
      partitions(i) = Cell(i, StarkUtils.fromEnvelope(children.get(i)))
      i += 1
    }

    new RTreePartitioner(partitions, minMax._1, minMax._2, minMax._3, minMax._4, pointsOnly)
  }

  def apply(samples: Array[MBR], maxCost: Int, pointsOnly: Boolean = true): RTreePartitioner =
    RTreePartitioner(samples, maxCost, GridPartitioner.getMinMax(samples), pointsOnly)
}


class RTreePartitioner private(_partitions: Array[Cell],
                               _minX: Double, _maxX: Double, _minY: Double, _maxY: Double,
                               pointsOnly: Boolean)
  extends GridPartitioner(_partitions, _minX,_maxX,_minY, _maxY) {

  override def partitionBounds(idx: Int) = partitions(idx)

  override def partitionExtent(idx: Int) = partitions(idx).extent

  override def printPartitions(fName: Path): Unit =
    GridPartitioner.writeToFile(partitions.map(_.range.wkt).zipWithIndex.map{ case (wkt, idx) => s"$idx;$wkt"}, fName)

  override def numPartitions = partitions.length

  override def getPartitionId(key: Any) = {
    val g = key.asInstanceOf[STObject]

    val env = StarkUtils.fromEnvelope(g.getGeo.getEnvelopeInternal)

    val part = partitions.find(_.extent.contains(env))

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

      val c = StarkUtils.getCenter(g.getGeo)

      val pX = c.getX
      val pY = c.getY
      val pc = NPoint(pX, pY)


      for(partition <- partitions) {
        val dist = partition.range.dist(pc)
        if(first || dist < minDist) {
          minDist = dist
          minPartitionId = partition.id
          first = false
        }
      }

      (minPartitionId, true)
    }

    if(outside) {
      partitions(partitionId).range = partitions(partitionId).range.extend(env)

//      if(!pointsOnly)
        partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
    } else if(!pointsOnly) {
      partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
    }

    partitionId

//    partitions.find{ case Cell(_,_,extent) => extent.contains(pc)}.map(_.id).getOrElse{
//      throw new IllegalStateException(s"Could not find any partition for $g")
//    }
  }


}
