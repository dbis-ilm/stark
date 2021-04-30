package dbis.stark.spatial

import java.io.File

import dbis.stark.STObject.MBR
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.Index
import dbis.stark.spatial.partitioner.{GridPartitioner, PartitionerConfig, PartitionerFactory, SpatialPartitioner}
import dbis.stark.visualization.Visualization
import dbis.stark.{Distance, STObject}
import javax.imageio.ImageIO
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

abstract class SpatialRDDFunctions[G <: STObject : ClassTag, V : ClassTag](rdd: RDD[(G,V)]) extends PairRDDFunctions[G,V](rdd) {

  def partitionBy(strategy: PartitionerConfig) = {
    PartitionerFactory.get(strategy, rdd) match {
      case Some(partitioner) => rdd.partitionBy(partitioner)
      case None => rdd
    }

//    rdd.map{ case (g,v) =>
//      val partId = partitioner.getPartition(g)
//      (partId, (g,v))
//    }
////      .repartition(partitioner.numPartitions)
//      .partitionBy(new Partitioner{
//      override def numPartitions = partitioner.numPartitions
//
//      override def getPartition(key: Any) = key.asInstanceOf[Int]
//    })
//      .map(_._2)




  }


  def intersects(qry: G): RDD[(G,V)]

  def covers(o: G): RDD[(G, V)]
  def coveredby(o: G): RDD[(G, V)]

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
  def knnTake(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance,V))]
  def knnAgg(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))]
  def knnAggIter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance,V))]
  def knnAgg2Iter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance,V))]
  def knn2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G,(Distance, V))]

  def visualize(imageWidth: Int, imageHeight: Int,
                path: String,
                fileExt: String = "png",
                range: (Double,Double,Double,Double) = GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal)),
                flipImageVert: Boolean = false,
                bgImagePath: String = null,
                pointSize: Int = 1,
                fillPolygon: Boolean = false,
                worldProj: Boolean = false) = {

    val vis = new Visualization()
    val jsc = new JavaSparkContext(rdd.context)

    val env = new MBR(range._1, range._2, range._3, range._4)

    var width = imageWidth
    var height = imageHeight

    if(bgImagePath != null) {
      val bg = ImageIO.read(new File(bgImagePath))
      width = bg.getWidth()
      height = bg.getHeight()
    }

    vis.visualize(jsc, rdd, width, height, env, flipImageVert, path, fileExt, bgImagePath, pointSize, fillPolygon, worldProj)
  }

  /**
   * Join this SpatialRDD with another (spatial) RDD.
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToManyPartitioning: Boolean): RDD[(V,V2)]
  def join[V2 : ClassTag](other: RDD[(G, V2)], predicate: JoinPredicate, partitioner: Option[GridPartitioner], oneToManyPartitioning: Boolean): RDD[(V,V2)]

  def broadcastJoin[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)]
  def broadcastJoinL[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)]
  def broadcastJoinWithIndex[V2 : ClassTag](other: RDD[Index[(G,V2)]], pred: JoinPredicate): RDD[(V, V2)]

  def knnJoin[V2: ClassTag](other: RDD[Index[V2]], k: Int, distFunc: (STObject,STObject) => Distance): RDD[(V,V2)]




  def zipJoin[V2 : ClassTag](other: RDD[(G,V2)], pred: JoinPredicate): RDD[(V, V2)]

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
