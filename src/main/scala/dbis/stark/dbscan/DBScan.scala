package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd._
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A Spark implementation of DBSCAN. The implementation is a Spark adaption of the approach
  * described in He, Yaobin, et al. "MR-DBSCAN: a scalable MapReduce-based DBSCAN algorithm
  * for heavily skewed data".
  *
  * @param eps the epsilon parameter defining the eps-neighborhood
  * @param minPts the MinPts parameter describing the minimum number of points required
  *               to form a dense region
  */
class DBScan[K, T : ClassTag](var eps: Double = 0.1, var minPts: Int = 10) extends java.io.Serializable {
  var ppd: Int = 6
  var maxPartitionSize: Int = 100

  /**
    * The default distance function for clustering (euclidean distance).
    *
    * @param v1 a vector representing the first point
    * @param v2 a vector representing the second point
    * @return the distance between the two points
    */
  def euclideanDistance(v1: Vector, v2: Vector): Double = {
    var d = 0.0
    for (i <- 0 until v1.size) { d += (v1(i) - v2(i)) * (v1(i) - v2(i)) }
    Math.sqrt(d)
  }

  /**
    * Because we allow user defined distance functions, we use a reference
    * to the function object here.
    */
  var distanceFun: (Vector, Vector) => Double = euclideanDistance

  /*-----------------------------------------------------------------------------------------------------*/
  /*                                                                                                     */
  /*                               The main method for implementing DBSCAN and                           */
  /*                               constructing a DBScanModel.                                           */
  /*                                                                                                     */
  /*-----------------------------------------------------------------------------------------------------*/

  def run(input: RDD[(K, Vector, T)]): DBScanModel[K,T] = {
    /*
     * step 1: determine the optimal partitioning, i.e. a list of MBBs describing the
     *         partitions in the n-dimensional space and send it around as broadcast
     *         variable
     */
    val globalMBB = getGlobalMBB(input.map { case (_, v, _) => v })
    val data = input.map{ case (id, v, p) => new ClusterPoint(id, v, payload = Some(p))}
    performClustering(data, globalMBB)
  }

  /**
    * Compute partitions for the whole dataset. Depending on the parameter either a
    * regular grid partitioning is used (if ppd != 0 && maxPartitionSize == 0) or
    * a binary space partitioning.
    *
    * @param globalMBB the MBB describing the dimensions of the whole dataset
    * @param input the input dataset needed for sampling
    * @return the list of MBBs describing the partitions
    */
  private def computePartitioning(globalMBB: MBB, input: RDD[ClusterPoint[K,T]]): List[MBB] = {
    val partitioner = if (maxPartitionSize > 0) {
      //logInfo("step 1: calculating the partitioning using the binary space partitioner")
      new BSPartitioner()
        .setMBB(globalMBB)
        .setCellSize(eps * 2.0)
        .setMaxNumPoints(maxPartitionSize)
        .computeHistogam(input, eps * 2.0)
    }
    else {
      //logInfo(s"step 1: calculating the partitioning using the grid partitioner with $ppd partitions per dimension")
      new GridPartitioner()
        .setMBB(globalMBB)
        .setPPD(ppd)
    }
    partitioner.computePartitioning()
  }

  /**
    * Starts the actual clustering by reading the input RDD and performing the following steps:
    *   1. determine a partitioning of the data space with overlapping partitions
    *   2. shuffle the points to assign them to their partitions
    *   3. perform a DBSCAN in each partition
    *   4. identify merge points, i.e. points belonging the multiple partitions and assigned
    *      to different clusters
    *   5. using this overlap information, derive a mapping between the cluster ids of the
    *      different partitions
    *   6. map the local cluster ids to global ids
    *   7. construct a DBScanModel representing the clustering result
    *
    * @param input the input RDD consisting of Vectors (of an arbitrary number of dimensions)
    * @return a clustering model containing the objects with their cluster id as label
    */
	private def performClustering(input: RDD[ClusterPoint[K,T]], globalMBB: MBB): DBScanModel[K,T] = {
    /*
     * step 1: determine the optimal partitioning, i.e. a list of MBBs describing the
     *         partitions in the n-dimensional space and send it around as broadcast
     *         variable
     */
    // val globalMBB = getGlobalMBB(input)
    //logInfo(s"step 0: determining global MBB: $globalMBB")

    /*
    val partitioner = new GridPartitioner().setMBB(globalMBB).setPPD(4)
    val partitionMBBs = partitioner.computePartitioning()
    */
    val partitionMBBs = computePartitioning(globalMBB, input)
    //logInfo(s"number of partitions: ${partitionMBBs.length}")
    // sc.parallelize(partitionMBBs, 1).saveAsTextFile("mbb")

    // we expand all partitions by epsilon
    partitionMBBs.foreach(mbb => mbb.expand(eps))
    val broadcastMBBs = input.sparkContext.broadcast(partitionMBBs)

    /*
     * step 2: assign the points to their partition
     */
    //logInfo("step 2: partitioning the input")
    val mappedPoints = partitionInput(input, partitionMBBs)

    /*
     * step 3: in each partition we perform DBSCAN
     */
    //logInfo("step 3: start local DBSCAN")
    val clusterSets = mappedPoints.groupBy(k => k._1) //FIXME reduceByKey?
    // TODO: try .repartition(partitionMBBs.length)
    val clusterResults = clusterSets.mapPartitions(iter => applyLocalDBScan(iter, broadcastMBBs.value), preservesPartitioning = true)
    val clusteredData = clusterResults.flatMap{case (_, x) => x}

    /*
     * we need the data twice, so let's cache them.
     */
    clusteredData.cache() // persist(StorageLevel.MEMORY_AND_DISK_SER)

//    println(s"clustered data : ${clusteredData.count()}")

    /*
    * step 4: we identify all points assigned to multiple clusters
    *         for this purpose we have to consider only merge candidates
    */
    //logInfo("step 4: determine merge points")
    val clusterGroups = clusteredData.filter(p => p.isMerge)
                                      .map(point => (point.vec, point)).groupByKey()


//    val s = clusterGroups.map{ case (k,v) => s"$k -> ${v.mkString(",")}" }.collect().mkString("\n")

//    println(s"cluster groups: $s")

    /*
     * and derive a mapping for cluster ids: if there exist a point appearing multiple times
     * with different cluster ids we have to add it to the map
     */
    val clusterPairs = clusterGroups.mapPartitions{ iter => buildClusterPairs(iter)}
//      .coalesce(1, shuffle = true)
//      .collect
      .distinct
        .collect() // FIXME: Bad! will collect all data in the driver! may be way too large!

    /*
     * if a point appears in multiple partitions we take only one instance of it
     */
    val mergedPoints = clusterGroups.mapPartitions { iter => eliminateDuplicates(iter) }

    /*
     * step 5: we calculate the transitive closure for global mapping and send it
     *         around as a broadcast variable
     */
    //logInfo("step 5: compute global mapping")
    val globalClusterMap = computeGlobalMapping(clusterPairs)
    val broadcastMapping = input.sparkContext.broadcast(globalClusterMap)

    /*
     * step 6: we combine the points belonging to exactly one partition with the merged
     *         points
     */
    //logInfo("step 6: compute the final clustering")
    val finalClustering = clusteredData.filter(! _.isMerge)
      .union(mergedPoints)
      /*
       * step 7: we translate the cluster id of all points according the global map
       */
     .map{ point => mapClusterId(point, broadcastMapping.value) }

    /*
     * step 8: finally, we construct a clustering model
     */
    //logInfo("step 7: construct clustering model")
    val partitionRDD = input.sparkContext.parallelize(partitionMBBs)
    new DBScanModel(finalClustering, partitionRDD, distanceFun)
	}

  /*-----------------------------------------------------------------------------------------------------*/
  /*                                                                                                     */
  /*                               Some helper methods for setting parameters.                           */
  /*                                                                                                     */
  /*-----------------------------------------------------------------------------------------------------*/
  /**
    * Sets the epsilon parameter of DBSCAN.
    *
    * @param e the new value for epsilon
    * @return the DBSCAN instance
    */
  def setEpsilon(e: Double) = {
    this.eps = e
    this
  }

  /**
    * Sets the MinPts parameter of DBSCAN.
    *
    * @param num the new value for MinPts
    * @return the DBSCAN instance
    */
  def setMinPts(num: Int) = {
    this.minPts = num
    this
  }

  /**
    * Sets the distance function for DBSCAN. A distance function has the signature (Vector, Vector) => Double
    *
    * @param f the distance function
    * @return the DBSCAN instance
    */
  def setDistanceFunc(f: (Vector, Vector) => Double) = {
    this.distanceFun = f
    this
  }

  /**
    * Sets the number of partitions per dimension for partitioning the dataset.
    * Switches to a equal-sized grid partition strategy.
    *
    * @param n the number of partitions per dimension
    * @return the DBSCAN instance
    */
  def setPPD(n: Int) = {
    ppd = n
    maxPartitionSize = 0
    this
  }

  /**
    * Sets the maximum size of partitions for partitioning the dataset.
    * Switches to a binary space partitioning strategy.
    *
    * @param n the maximum size of partitions
    * @return the DBSCAN instance
    */
  def setMaxPartitionSize(n: Int) = {
    maxPartitionSize = n
    ppd = 0
    this
  }
  /*-----------------------------------------------------------------------------------------------------*/
  /*                                                                                                     */
  /*                                 Functions for partitioning the data.                                 */
  /*                                                                                                     */
  /*-----------------------------------------------------------------------------------------------------*/
  /**
    * Computes the mininum bounding box of the given data set.
    *
    * @param input an RDD representing the input data
    * @return the MBB for the data set
    */
  protected[dbscan] def getGlobalMBB(input: RDD[Vector]): MBB = input.aggregate(MBB.zero)(MBB.mbbSeq, MBB.mbbComb)

  /**
    * Returns the indices of the partitions (i.e. the partition number) containing the point described by vec.
    *
    * @param partitions the list of partitions where the position in the list represents the partition id
    * @param vec the point to be checked
    * @return the ids of the containing partition
    */
  protected[dbscan] def containingPartitions(partitions: List[MBB], vec: Vector): List[Int] =
    // note: merge points may belong to more than one partition!
    partitions.zipWithIndex.filter{ case (mbb, id) => mbb.contains(vec)}.map{ case (m,i) => i}

  /**
    * Partitions the input data according to the partitions represented by MBBs. For now, we create
    * only pairs of partition number and points which are physically partitioned later by a  groupBy.
    *
    * @param input the input RDD
    * @param partitions the list of MBBs representing the partitions
    * @return an RDD of pairs of partition-id (the index of the partition in the MBB list) and
    *         the point
    */
  protected[dbscan] def partitionInput(input: RDD[ClusterPoint[K,T]], partitions: List[MBB]): RDD[(Int,ClusterPoint[K,T])] =
    input.flatMap{ point =>
      val partitionList = containingPartitions(partitions, point.vec)
      require(partitionList.nonEmpty,"no partition found!")
      partitionList.map(p => (p, ClusterPoint(point, merge = partitionList.length > 1, pload = point.payload)))
    }

  /*-----------------------------------------------------------------------------------------------------*/
  /*                                                                                                     */
  /*                               Functions for local DBSCAN processing.                                */
  /*                                                                                                     */
  /*-----------------------------------------------------------------------------------------------------*/
  /**
    * Expands the cluster starting at point p by considering the given list of points. If p is a core
    * point then clusterID is assigned to it. All other points belonging to the same cluster get the
    * also clusterID.
    *
    * @param p the starting point
    * @param clusterID the next available cluster identifier
    * @param points the list of points to be considered
    * @return true if p belongs to a cluster and isn't noise
    */
  protected[dbscan] def expandCluster(p: ClusterPoint[K,T], clusterID: Int, points: Stream[ClusterPoint[K,T]]) : Boolean = {
    // we consider only points within the eps distance
    var seeds = points.filter(l => distanceFun(p.vec, l.vec) < eps).toBuffer

    if (seeds.size < minPts) {
      // if we don't have enough points in our epsilon distance then p is considered as noise
      p.label = ClusterLabel.Noise
      p.clusterId = 0
      return false
    }
    // all points in seeds are assigned to the same cluster
    seeds.foreach(x => x.clusterId = clusterID)
    // we remove p from seeds
    seeds -= p
    // we proceed with the points in seed
    while (seeds.nonEmpty) {
      val o = seeds.head
      val npts = points.filter(l => distanceFun(o.vec, l.vec) < eps)
      // if o is a core object
      if (npts.size >= minPts) {
        o.label = ClusterLabel.Core
        // consider only noise and unclassified points
        val pp = npts.filter(n => n.label == ClusterLabel.Noise || n.label == ClusterLabel.Unclassified)
        // from these points we add all unclassified points to seed
        seeds ++= pp.filter(n => n.label == ClusterLabel.Unclassified)
        // .. and assign them to our cluster
        pp.foreach(n => { n.clusterId = clusterID; n.label = ClusterLabel.Reachable })
      }
      // done with o, continue ...
      seeds -= o
    }
    true
  }

  /**
    * Performs a local DBSCAN on a list of points.
    *
    * @param points a list of points to be clustered
    * @param startId the first available cluster identifier
    * @return the list of clustered points (i.e. label and clusterId are set now)
    */
  protected[dbscan] def localDBScan(points: Stream[ClusterPoint[K,T]], startId: Int) : Stream[ClusterPoint[K,T]] = {
    // prepare the next available cluster id
    var nextClusterID: Int = startId + 1

    points.foreach(p => {
      // we choose a point not yet seen as a potential cluster seed
      // and try to expand the cluster
      if (p.label == ClusterLabel.Unclassified) {
        if (expandCluster(p, nextClusterID, points))
          // because we have constructed a cluster we need a new cluster id
          nextClusterID += 1
      }
    })
    // the points are assigned to clusters now
    points
  }

  /**
    * Processes a list of (partitionId, (partitionId, list-of-points)) by applying a local
    * DBSCAN to each list of points. This function is mainly a wrapper for the actual local
    * DBSCAN to process partitioned RDDs with a group key (partition id).
    *
    * @param iter an iterator on the list of tuples of partition-id, list-of-points
    * @param mbbs the list of MBBs describing the partitioning of data
    * @return an iterator on the list of tuples of partition-id, list-of-clustered-points
    */
  protected[dbscan] def applyLocalDBScan(iter: Iterator[(Int, Iterable[(Int, ClusterPoint[K,T])])], mbbs: List[MBB]):
    Iterator[(Int, Iterable[ClusterPoint[K,T]])] = {


    iter.map{ case (partitionId, objIter) =>

      // determine a new starting cluster id for the partition
      // here we assume to not having more than 1000 clusters
      val startId = partitionId * 1000
      val points = objIter.map(_._2).toStream

      // perform the actual clustering
      val data = localDBScan(points, startId)

      // check for border points, i.e. if a point is at the border of mbbs(idx)
      (partitionId, data)
    }

//    val clusteredData = ListBuffer[(Int,Iterable[ClusterPoint[K,T]])]()
//
//    while (iter.hasNext) {
//      val (partitionId, objIter) = iter.next()
//      // determine a new starting cluster id for the partition
//      // here we assume to not having more than 1000 clusters
//      val startId = partitionId * 1000
//
//      val points = objIter.map(pair => pair._2)
//
//      // perform the actual clustering
//      val data = localDBScan(points, startId)
//
//      // check for border points, i.e. if a point is at the border of mbbs(idx)
//      clusteredData += ((partitionId, data))
//
//      //logInfo(s"computeLocalDBScan for partition #$partitionId with ${points.size} objects")
//    }
//    clusteredData.iterator
  }

  /*-----------------------------------------------------------------------------------------------------*/
  /*                                                                                                     */
  /*           Functions for global processing, i.e. constructing and analyzing the                      */
  /*           graph of local cluster id mappings.                                                       */
  /*                                                                                                     */
  /*-----------------------------------------------------------------------------------------------------*/

  /**
    * Maps the locally computed cluster id of the given point using the global map of cluster ids. entries
    * is a table of [local-id -> global-id] entries.
    *
    * @param point the point whose cluster id is translated
    * @param entries the global mapping table
    * @return the point with the new cluster id
    */
  protected[dbscan] def mapClusterId(point: ClusterPoint[K,T], entries: Map[Int, Int]) : ClusterPoint[K,T] = {
    // if we can find the local cluster id in the table we take the global one,
    // otherwise there is no need to map it (i.e. the point wasn't assigned to multiple clusters
    val clusterID = if (entries.isDefinedAt(point.clusterId)) entries(point.clusterId) else point.clusterId
    point.clusterId = clusterID
    point
  }

  /**
    * Compute a global mapping between cluster ids from a list of mapping pairs. This is needed because
    * a point may be assigned to multiple clusters with different cluster ids.
    *
    * @param pairs a list of pairs of cluster ids representing the same cluster
    * @return a global mapping table for cluster ids
    */
  protected[dbscan] def computeGlobalMapping(pairs: Array[(Int, Int)]) : Map[Int, Int] = {
    // a helper function for generating a sequence of unique ids
    val nextId = { var i = 100000; () => { i += 1; i} }

    val map = scala.collection.mutable.Map[Int, Int]()
    val graph = Graph[Int,UnDiEdge]()

    // first, we build a graph where nodes represent cluster ids
    // and edges represent mappings between them, i.e. both ids represent the
    // same cluster
    pairs.foreach{ case (one, two) => graph += one~two }

    // then, we process all nodes
    for (node <- graph.nodes) {
      // construct a set of nodes in its transitive closure
      var nodes = mutable.Set[Int]()
      for (p <- node.outerNodeTraverser) { nodes += p }
      // assign them the same new cluster id
      val id = nextId()
      // and remove the nodes from the graph
      nodes.foreach { n => { map += ((n, id)); graph -= n }}
    }
    map.toMap
  }

  /**
    * Constructs pairs of cluster ids from different local clusterings which map to each other.
    *
    * @param iter an iterator on tuples of partition-id, list-of-points
    * @return an iterator to a list of cluster-id mappings
    */
  protected[dbscan] def buildClusterPairs(iter: Iterator[(Vector, Iterable[ClusterPoint[K,T]])]) : Iterator[(Int, Int)] = {

    // TODO: is this better?
    val a = iter.flatMap(_._2).filter(_.clusterId > 0).toStream.groupBy(p => p.vec).iterator.flatMap{ case (_, idList) =>
      for {
         ClusterPoint(_,_,aId, _,_,_) <- idList
         ClusterPoint(_,_,bId, _,_,_) <- idList
         if aId < bId
        } yield (aId,bId)
    }

    a


//    //FIXE: how big can this Set get? may be too large for memory?
//    var clusterSet : mutable.Set[(Int, Int)] = mutable.Set()
//    var lastVec: Vector = Vectors.zeros(0)
//    var lastCluster: Int = -1
//    while (iter.hasNext) {
//      val (_, pointIter) = iter.next()
//
//      for (p <- pointIter)  {
//        if (p.clusterId > 0 && lastVec == p.vec && lastCluster != p.clusterId) {
//          clusterSet += ((lastCluster, p.clusterId))
//        }
//        if (p.clusterId > 0) {
//          lastVec = p.vec
//          lastCluster = p.clusterId
//        }
//      }
//    }
//    clusterSet.iterator
  }

  /**
    * Removes duplicate points from a sequence of labeled points. This function is used to process
    * points which have been assigned to multiple partitions and to keep only a single instance.
    *
    * @param iter an iterator on tuples of partition-id, list-of-points
    * @return an iterator to a list of unique point (contains only a single point)
    *
    */
  protected[dbscan] def eliminateDuplicates(iter: Iterator[(Vector, Iterable[ClusterPoint[K,T]])]) : Iterator[ClusterPoint[K,T]] = {
    
    val seenIds = mutable.Set.empty[K]
    
    iter.flatMap{ case (_, pointIter) => pointIter.filter{ p => 
        val res = !seenIds.contains(p.id)
        seenIds += p.id
        res
      } 
    }
  }

}
