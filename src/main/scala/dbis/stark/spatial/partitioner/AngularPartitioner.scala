package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject

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

}

object SphereCoordinate {

  def apply(so: STObject): SphereCoordinate = {
    val r = math.sqrt( math.pow(so.getMaxX,2) + math.pow(so.getMaxY, 2))
    val theta = math.atan2(so.getMaxY, so.getMaxX)
    SphereCoordinate(r, theta)
  }

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
class AngularPartitioner(
    dimensions: Int,
    ppD: Int,
    firstQuadrantOnly: Boolean = false) extends SpatialPartitioner {

  require(dimensions == 2)
  require(ppD > 1)

  /* Phi is the angle width of a partition and depends on the number of partitions to create as well as
   * on the region to cover. if [[firstQuadrantOnly]] is true, then we only have to cover 90 degree - used
   * for Skyline computation. Otherweise we have to cover the whole 360 degree.
   *
   * Since the angles are computed based in math.Pi the start value defines the minimal possible value expected.
   * Angles start at 0 for [[firstQuadrantOnly]] or -Pi otherwise.
   *
   */
  val (start,phi) = if(firstQuadrantOnly) (0.0, (math.Pi / 2)/ ppD.toDouble) else (-math.Pi,(2 * math.Pi)/ ppD.toDouble )

  override def numPartitions: Int = ppD

  override def getPartitionId(key: Any): Int = {
    val so = key.asInstanceOf[STObject]
    val g = SphereCoordinate(so)

    var step = 1
    while(start +(step*phi) < g.angle) {
      step += 1
    }

    step-1
  }

  override def printPartitions(fName: Path): Unit = {

  }

  override def getIntersectingPartitionIds(o: STObject): Array[Int] = ???
}