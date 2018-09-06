package dbis.spark.spatial

import java.nio.file.Path

import dbis.stark.spatial.NPoint
import dbis.stark.spatial.partitioner.SpatialPartitioner

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Represents spherical coordinates in n dimensional space
 * 
 * @param radial The radial coordinate, i.e. the distance to the center 
 * @param angle The list of angles in each dimension
 */
case class SphereCoordinate(radial: Double, angle: Double) {

  /**
   * Checks all angles of if this coordinate are greater or equal than from the other coordinate
   */
  def >=(other: SphereCoordinate): Boolean = angle >= other.angle

  /**
   * Checks all angles of if this coordinate are smaller than from the other coordinate
   */
  def <(other: SphereCoordinate): Boolean = angle < other.angle

//  override def toString = s"SphereCoordinate($radial; $angle)"

}

object SphereCoordinate {

  def apply(point: NPoint): SphereCoordinate = apply(point.c)

  /**
   * Convert the given point, given as an array of cartesian coordinate values
   * into a spherical coordinate
   */
  def apply(cartesian: Array[Double]): SphereCoordinate = {

    require(cartesian.length == 2, "only 2d supported currently")

    val r = math.sqrt( cartesian.map(x => math.pow(x, 2)).sum )

    val theta = math.atan2(cartesian(1), cartesian(0))

    SphereCoordinate(r, theta)
  }
}

/**
 * A region with a
 */
case class SphereRegion(start: SphereCoordinate, end: SphereCoordinate) {

  def contains(p: SphereCoordinate) = p >= start && p < end

}

/**
 * Angular Space Partitioning
 *
 * @param dimensions The number of dimensions in cartesian space
 * @param ppD The number of partition to generate in each dimension
 */
class AngularPartitioner[V: ClassTag](
    dimensions: Int,
    ppD: Int,
    firstQuadrantOnly: Boolean = false) extends SpatialPartitioner {

  require(dimensions == 2)
  require(ppD > 1)


  /*
   * We use this map to associate a hash value of the
   * angular coordinates to a partition ID.
   * We need this, because the ID must be in 0..numPartitions
   * but the hash value is way beyond that.
   *
   * Although this is a simple solution, this causes that
   * there is no ordering between the partitions, i.e. two
   * neighbored partitions will most likely not have successive
   * IDs
   */
  private val theMap = mutable.Map.empty[Int, Int]


  def getId(p: SphereCoordinate) = {

    val h = (p.angle / phi).toInt.hashCode()
    theMap.getOrElseUpdate(h, => {
      ???
    })
    var id: Int = -1 
    theMap.get(h) match {
      case Some(h2) =>
        id = h2
      case None =>
        id = theMap.size
        theMap += (h -> id)
    }

    id
  }
  
  // partitions in Dimension 0 
  val phi = (if(firstQuadrantOnly) 90 else 360) / ppD.toDouble
  
  override def numPartitions: Int = ppD
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[SphereCoordinate]
    
    val cellId = getId(g)
    
    cellId
  }

  override def printPartitions(fName: Path): Unit = {

  }
}