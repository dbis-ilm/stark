package dbis.spark.spatial

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import scala.collection.mutable.Map

/**
 * Represents spherical coordinates in n dimensional space
 * 
 * @param radial The radial coordinate, i.e. the distance to the center 
 * @param angulars The list of angles in each dimension
 */
case class SphereCoordinate(radial: Double, angulars: Array[Double]) {
  
  /**
   * Checks all angles of if this coordinate are greater or equal than from the other coordinate 
   */
  def >=(other: SphereCoordinate): Boolean = other.angulars.zipWithIndex.forall{ case (c,i) => c >= angulars(i) }
  
  /**
   * Checks all angles of if this coordinate are smaller than from the other coordinate
   */
  def <(other: SphereCoordinate): Boolean = angulars.zipWithIndex.forall { case (c,i) => other.angulars(i) < c }
 
  override def toString() = s"SphereCoordinate($radial; ${angulars.mkString(",")})"
 
}

object SphereCoordinate {
  
  private def square(x: Double) = math.pow(x, 2)
  
  /**
   * Convert the given point, given as an array of cartesian coordinate values
   * into a spherical coordinate
   */
  def apply(cartesian: Array[Double]): SphereCoordinate = {
    
    val r = math.sqrt( cartesian.map(square(_)).sum )
    
    val angles = (0 until cartesian.size - 1).map { i =>
      
      val tanPhi = math.sqrt(cartesian.view(i+1, cartesian.size).map(square(_)).sum) / cartesian(i)
      
      math.atan(tanPhi)
    }.toArray
    
    SphereCoordinate(r, angles)
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
    ppD: Int) extends SpatialPartitioner {
  
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
  private val theMap = Map.empty[Int, Int]
  
  
  def getId(p: SphereCoordinate) = {

    val h = p.angulars.map { x => (x / phi).toInt }.deep.hashCode()
    
    var id: Int = -1 
    val h2 = theMap.get(h)
    if(h2.isDefined)
      id = h2.get
    else {
      id = theMap.size
      theMap += (h -> id)
    }
      
    id
  }
  
  // partitions in Dimension 0 
  val phi = 360 / ppD.toDouble
  
  override def numPartitions: Int = (dimensions - 1) * ppD
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[SphereCoordinate]
    
    val cellId = getId(g)
    
    cellId
  }
}