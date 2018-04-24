package dbis.stark.spatial

import dbis.stark.STObject.MBR

import scala.reflect.ClassTag
import dbis.stark.{Distance, STObject}
import dbis.stark.spatial.partitioner.SpatialPartitioner
import dbis.stark.visualization.Visualization
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

abstract class SpatialRDDFunctions[G <: STObject : ClassTag, V : ClassTag](rdd: RDD[(G,V)]) extends Serializable {
  
  def intersects(qry: G): RDD[(G,V)]

  /**
   * Find all elements that are contained by a given query geometry
   */
  def containedby(qry: G): RDD[(G,V)]

  /**
   * Find all elements that contain a given other geometry
   */
  def contains(o: G): RDD[(G,V)]


  def withinDistance(qry: G, maxDist: Distance, distFunc: (STObject,STObject) => Distance): RDD[(G,V)]
      
  def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))]


  def visualize(imageWidth: Int, imageHeight: Int,
                path: String,
                fileExt: String = "png",
                range: (Double,Double,Double,Double) = SpatialPartitioner.getMinMax(rdd),
                flipImageVert: Boolean = false,
                pointSize: Int = 1) = {

    val vis = new Visualization()
    val jsc = new JavaSparkContext(rdd.context)

    val env = new MBR(range._1, range._2, range._3, range._4)

    vis.visualize(jsc, rdd, imageWidth, imageHeight, env, flipImageVert, path, fileExt, pointSize)
  }

  /**
   * Join this SpatialRDD with another (spatial) RDD.
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean): RDD[(V,V2)]
  def join[V2 : ClassTag](other: RDD[(G, V2)], predicate: JoinPredicate.JoinPredicate, partitioner: Option[SpatialPartitioner]): RDD[(V,V2)]

  def skyline(ref: STObject,
              distFunc: (STObject, STObject) => (Distance, Distance),
              dominates: (STObject, STObject) => Boolean,
              ppD: Int,
              allowCache: Boolean): RDD[(G,V)]

  def skylineAgg(ref: STObject,
              distFunc: (STObject, STObject) => (Distance, Distance),
              dominates: (STObject, STObject) => Boolean
              ): RDD[(G,V)]
  
  /**
   * Cluster this SpatialRDD using DBSCAN
   *
   * @param minPts DBSCAN minimum points parameter
   * @param epsilon DBSCAN epsilon parameter
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
