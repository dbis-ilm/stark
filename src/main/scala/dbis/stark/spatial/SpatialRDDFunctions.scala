package dbis.stark.spatial

import scala.reflect.ClassTag
import dbis.stark.STObject
import org.apache.spark.rdd.RDD

abstract class SpatialRDDFunctions[G <: STObject : ClassTag, V : ClassTag] extends Serializable {
  
  def intersects(qry: G): RDD[(G,V)]
    

  /**
   * Find all elements that are contained by a given query geometry
   */
  def containedby(qry: G): RDD[(G,V)]

  /**
   * Find all elements that contain a given other geometry
   */
  def contains(o: G): RDD[(G,V)]

  def withinDistance(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double): RDD[(G,V)]
      
  def kNN(qry: G, k: Int): RDD[(G,(Double,V))]   
  
  
  /**
   * Join this SpatialRDD with another (spatial) RDD.
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean): RDD[(V,V2)]
  
  
  /**
   * Cluster this SpatialRDD using DBSCAN
   *
   * @param minPts
   * @param epsilon
   * @param keyExtractor A function that extracts or generates a unique key for each point
   * @param includeNoise A flag whether or not to include noise points in the result
   * @param maxPartitionCost Maximum cost (= number of points) per partition
   * @param outfile An optional filename to write clustering result to
   * @return Returns an RDD which contains the corresponding cluster ID for each tuple
   */
  def cluster[KeyType](
		  minPts: Int,
		  epsilon: Double,
		  keyExtractor: ((G,V)) => KeyType,
		  includeNoise: Boolean = true,
		  maxPartitionCost: Int = 10,
		  outfile: Option[String] = None
		  ) : RDD[(G, (Int, V))]
}