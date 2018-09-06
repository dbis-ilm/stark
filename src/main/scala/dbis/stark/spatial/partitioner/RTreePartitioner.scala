package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.{Cell, NPoint, Utils}
import org.locationtech.jts.index.strtree.Boundable

import scala.collection.JavaConverters._

class RTreePartitioner[G <: STObject,V](samples: Seq[(G,V)],
                                        _minX: Double, _maxX: Double, _minY: Double, _maxY: Double,
                                        maxCost: Int, pointsOnly: Boolean)
  extends SpatialPartitioner(_minX,_maxX,_minY, _maxY) {

  require(maxCost > 0)

  def this(samples: Seq[(G,V)], maxCost: Int, minMax: (Double, Double, Double, Double), pointsOnly: Boolean) =
    this(samples, minMax._1, minMax._2, minMax._3, minMax._4, maxCost, pointsOnly)

  def this(samples: Seq[(G,V)], maxCost: Int, pointsOnly: Boolean = true) =
    this(samples, maxCost, SpatialPartitioner.getMinMax(samples.iterator), pointsOnly)

  protected[spatial] val partitions: Array[Cell] = {

    val dummy: Byte = 0x0

    val capacity = math.max(samples.length / maxCost, 2)
    val tree = new RTree[Byte](capacity)

    samples.foreach { case (g, _) =>
      tree.insert(g.getGeo, dummy)
    }

    val children = tree.getRoot.getChildBoundables.iterator().asScala

    children.zipWithIndex.map{ case (child, idx) =>
      val mbr = child.asInstanceOf[Boundable].getBounds.asInstanceOf[MBR]
      Cell(idx, Utils.fromEnvelope(mbr))
    }.toArray
  }


  override def partitionBounds(idx: Int) = partitions(idx)

  override def partitionExtent(idx: Int) = partitions(idx).extent

  override def printPartitions(fName: Path): Unit =
    writeToFile(partitions.map(_.range.wkt).zipWithIndex.map{ case (wkt, idx) => s"$idx;$wkt"}, fName)

  override def numPartitions = partitions.length

  override def getPartition(key: Any) = {
    val g = key.asInstanceOf[STObject]

    val env = Utils.fromEnvelope(g.getGeo.getEnvelopeInternal)

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

      val c = Utils.getCenter(g.getGeo)

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
        partitions(partitionId).extendBy(Utils.fromGeo(g.getGeo))
    }


    partitionId


//    partitions.find{ case Cell(_,_,extent) => extent.contains(pc)}.map(_.id).getOrElse{
//      throw new IllegalStateException(s"Could not find any partition for $g")
//    }
  }
}
