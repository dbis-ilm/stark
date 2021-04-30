package org.apache.spark

import java.nio.file.Paths

import dbis.stark.STObject.MBR
import dbis.stark.dbscan.{ClusterLabel, DBScan}
import dbis.stark.raster.{RasterRDD, Tile}
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial._
import dbis.stark.spatial.indexed._
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.spatial.partitioner._
import dbis.stark.{Distance, STObject}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


/**
 * A helper class used in the implicit conversions
 *
 * @param self The original RDD to treat as a spatial RDD
 */
class PlainSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
                                                                       self: RDD[(G,V)]
  ) extends SpatialRDDFunctions[G,V](self) with Serializable {

  val o = Ordering.fromLessThan[(G,(Distance,V))](_._2._1 < _._2._1)


  def saveAsStarkTextFile(path: String, formatter: ((G,V)) => String): Unit =
    self.partitioner.foreach{
      case sp: SpatioTempPartitioner[G] =>
        var i = 0
        var cnt = 0
        val strings = ListBuffer.empty[String]
        while(i < sp.numSpatialPartitions) {
          val (extent, intervals) = sp.getSTBounds(i)
          var j = 0

          var prevTStart = Long.MinValue
          while(j < intervals.length) {
            val partition = Array(extent.wkt, prevTStart.toString, intervals(j),"part-%05d".format(cnt)).mkString(STSparkContext.PARTITIONINFO_DELIM)
            strings += partition
            prevTStart = intervals(j)
            cnt += 1
            j += 1
          }
          i += 1
        }

        self.map(formatter).saveAsTextFile(path)
        self.sparkContext.parallelize(strings).saveAsTextFile(Paths.get(path,STSparkContext.PARTITIONINFO_FILE).toString)


      case sp: GridPartitioner =>
        val wkts = self.partitions.indices.map{ i =>
          Array(sp.partitionExtent(i).wkt,"","","part-%05d".format(i)).mkString(STSparkContext.PARTITIONINFO_DELIM)
        }

        self.map(formatter).saveAsTextFile(path)
        self.sparkContext.parallelize(wkts).saveAsTextFile(Paths.get(path,STSparkContext.PARTITIONINFO_FILE).toString)
      case _ =>
        self.saveAsTextFile(path)
    }




  def saveAsStarkObjectFile(path: String): Unit = self.partitioner.flatMap{
    case sp: GridPartitioner => Some(sp)
    case _ =>
      None
  } match {
    case Some(sp) =>
      val wkts = self.partitions.indices.map{ i =>
        Array(sp.partitionExtent(i).wkt,"","","part-%05d".format(i)).mkString(STSparkContext.PARTITIONINFO_DELIM)
      }

      self.saveAsObjectFile(path)
      self.sparkContext.parallelize(wkts).saveAsTextFile(Paths.get(path,STSparkContext.PARTITIONINFO_FILE).toString)
    case _ =>
      self.saveAsObjectFile(path)
  }

  /**
   * Find all elements that intersect with a given query geometry
   */
  override def intersects(qry: G): SpatialFilterRDD[G, V] = self.withScope{ new SpatialFilterRDD[G,V](self, qry, JoinPredicate.INTERSECTS) }

  /**
   * Find all elements that are contained by a given query geometry
   */
  override def containedby(qry: G): SpatialFilterRDD[G, V] = self.withScope{new SpatialFilterRDD[G,V](self, qry, JoinPredicate.CONTAINEDBY) }

  /**
   * Find all elements that contain a given other geometry
   */
  override def contains(o: G): SpatialFilterRDD[G, V] = self.withScope{new SpatialFilterRDD[G,V](self, o, JoinPredicate.CONTAINS) }

  override def covers(o: G): SpatialFilterRDD[G, V] = self.withScope{new SpatialFilterRDD[G,V](self, o, JoinPredicate.COVERS) }

  override def coveredby(o: G): SpatialFilterRDD[G, V] = self.withScope{new SpatialFilterRDD[G,V](self, o, JoinPredicate.COVEREDBY) }

  override def withinDistance(qry: G, maxDist: Distance, distFunc: (STObject,STObject) => Distance): SpatialFilterRDD[G, V] =
    self.withScope{ new SpatialFilterRDD(self, qry, PredicatesFunctions.withinDistance(maxDist, distFunc) _) }


  override def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))] = self.withScope{
    // compute k NN for each partition individually --> n * k results
    val knn = self.mapPartitions({iter => iter.map { case (g,v) =>
        val d = distFunc(g,qry)
        (g,(d,v)) // compute and return distance
      }
      .toList
      .sortWith(_._2._1 < _._2._1) // on distance
      .take(k) // take only the fist k
      .toIterator // remove the iterator
    }).sortBy(_._2._1, ascending = true)
      .take(k)


//    val knn = self.map{ case(g,v) =>
//        (distFunc(qry,g), (g,v))
//    }// compute distances and make it key
//                  .sortByKey(ascending = true) // sort by distance
//                  .take(k) // take only the first k elements
//                  .map{ case (d,(g,v)) => (g, (d,v))}  // project to desired format
//


    self.sparkContext.parallelize(knn)
  }

  override def knnTake(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance,V))] = {
    implicit val ord = new Ordering[(G,(Distance,V))] {
      override def compare(x: (G,(Distance,V)), y: (G,(Distance,V))) = if(x._2._1 < y._2._1) -1 else if(x._2._1 > y._2._1) 1 else 0
    }
    val knn = self.mapPartitions { iter =>
      iter.map { case (g, v) =>
        val d = distFunc(g,qry)
        (g,(d,v)) // compute and return distance
      }
    }.takeOrdered(k)

    self.sparkContext.parallelize(knn)
  }

  override def knnAgg2Iter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance,V))] = {
    knnAggIter(ref, k, distFunc)
  }

  override def knnAggIter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance,V))] = {
    type Data = (G,V)
    def combine(knn: KNN[Data], tuple: Data) = {
      val dist = distFunc(ref, tuple._1)
      knn.insert((dist,tuple))
      knn
    }

    def merge(knn1: KNN[Data], knn2: KNN[Data]) = {
      knn1.merge(knn2)
    }

    val empty = new KNN[Data](k)

    val knn = self.aggregate(empty)(combine, merge)
    knn.iterator.map{ case (dist,(g,v)) => (g,(dist,v)) }
  }

  override def knnAgg(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance,V))] =
    self.sparkContext.parallelize(knnAggIter(ref,k,distFunc).toSeq)

  override def knn2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G,(Distance, V))] = self.partitioner match {
    case Some(p: GridPartitioner) =>

      val partitionOfQry = p.getPartition(qry)

      def getPartitionsKNN(context: TaskContext, iter: Iterator[(G,V)]): KNN[(G,V)] = {
        val knnS = new KNN[(G, V)](k)
        if(iter.nonEmpty && context.partitionId() == partitionOfQry) {
          iter.foreach{o =>
            val dist = distFunc(qry, o._1)
            knnS.insert(dist, o)
          }
        }

        knnS
      }

      val knnsInPart = self.sparkContext.runJob(self, getPartitionsKNN _,Seq(partitionOfQry)).apply(0)

      // get the maximum distance of all points in the ref's partition
      val maxDist = if (knnsInPart.isEmpty) 0 // none found
      else
        knnsInPart.max._1.maxValue

      // make a box around ref point and get all intersecting partitions
      val qryPoint = qry.getGeo.getCentroid.getCoordinate
      val mbr = new MBR(qryPoint.x - maxDist, qryPoint.x + maxDist, qryPoint.y - maxDist, qryPoint.y + maxDist)
      val geoMBR = StarkUtils.makeGeo(mbr)
      val intersectinPartitions = p.getIntersectingPartitionIds(geoMBR)

      if(knnsInPart.size == k && intersectinPartitions.length == 1) {
        knnsInPart.iterator.map { case (d,(g,v)) => (g,(d,v))}
      } else if(knnsInPart.size > k && intersectinPartitions.length == 1) {
        knnsInPart.toStream.sortBy(_._1.minValue).map { case (d,(g,v)) => (g,(d,v))}.iterator
      } else {

        if (maxDist <= 0.0) {
          // for maxDist == 0 the box would be a point and not find any addtitional kNNs
          // we maybe should iteratively increase the box size
          //        println("fallback to knnAgg")
          knnAggIter(qry, k, distFunc)
        }
        else {
          println("found only myself in partition. perform global search!")

          val kNNsOfIntersecting = self.sparkContext.runJob(self, getPartitionsKNN _, intersectinPartitions)
          val numCandidates = kNNsOfIntersecting.iterator.map(_.size).sum

          val result: Iterator[(G,(Distance,V))] = if(numCandidates < k) {
            // we still havn't found enough...
            val knns = this.containedby(STObject(geoMBR).asInstanceOf[G])
              .map { case (g, v) => (g, (distFunc(qry, g), v)) }
              .takeOrdered(k)(o.reverse)

            knns.iterator
          } else {

            val knnsSorted = kNNsOfIntersecting.map(b => b.sortedArray.map{case (d,gv) => (gv,d)})

            KNN.merge(knnsSorted, k)

          }
//          val qryPoint = qry.getGeo.getCentroid.getCoordinate
//          val env = StarkUtils.makeGeo(
//            new MBR(qryPoint.x - maxDist, qryPoint.x + maxDist, qryPoint.y - maxDist, qryPoint.y + maxDist))
//          val knns = this.containedby(STObject(env).asInstanceOf[G])
//            .map { case (g, v) => (g, (distFunc(qry, g), v)) }
//            .takeOrdered(k)(o.reverse)
//          val result = (knns ++ knnsInPart.map { case (d,(g, v)) => (g, (d, v)) }).toStream.sorted(o).iterator.take(k)
          result
        }
      }

    case _ =>
      knnAggIter(qry,k,distFunc)
  }

  /**
   * Join this SpatialRDD with another (spatial) RDD.<br><br>
   * <b>NOTE:</b> There will be no partition pruning and basically all cartesian combinations have to be checked
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToManyPartitioning: Boolean): SpatialJoinRDD[G, V, V2] = self.withScope {
    val cleanF = self.context.clean(pred)
    new SpatialJoinRDD(self, other, cleanF, oneToManyPartitioning)
  }




  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[GridPartitioner] = None, oneToMany: Boolean = false): SpatialJoinRDD[G, V, V2] = self.withScope {
      new SpatialJoinRDD(
        if (partitioner.isDefined) self.partitionBy(partitioner.get) else self,
        if (partitioner.isDefined) other.partitionBy(partitioner.get) else other,
        pred, oneToMany = oneToMany)
  }

  /**
    * Performs a broadcast join. The relation "other" is broadcasted to all partitions of this RDD and thus, "other"
    * should be the smaller one and fit into memory!
    * @param other The smaller relation to join with - will be broadcasted
    * @param pred The join predicate
    * @tparam V2 Payload type in second relation
    * @return Returns an RDD with payload values from left and right
    */
  override def broadcastJoin[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)] = {
    val otherArray = other.collect()
    val otherBC = self.sparkContext.broadcast(otherArray)

    self.mapPartitions{iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val otherArr = otherBC.value
      iter.flatMap{ left =>
        otherArr.iterator.filter{ right =>
          predFunc(left._1, right._1)
        }.map{ case (_,v2) => (left._2, v2) }
      }
    }
  }

  override def broadcastJoinL[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)] = 
    broadcastJoin(other, pred)

  def broadcastJoinWithIndex[V2 : ClassTag](other: RDD[Index[(G,V2)]], pred: JoinPredicate): RDD[(V, V2)] = {
    val otherArray = other.collect()
    val otherBC = self.sparkContext.broadcast(otherArray)

    self.mapPartitions{iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val otherArr = otherBC.value

      iter.flatMap{ left =>
        otherArr.iterator.flatMap{ idx =>
          idx.query(left._1)
            .filter{ case (g,_) => predFunc(left._1, g) }
            .map{ case (_,v2) => (left._2, v2) }
        }
      }
    }
  }

  override def knnJoin[V2: ClassTag](other: RDD[Index[V2]], k: Int, distFunc: (STObject,STObject) => Distance): RDD[(V,V2)] = self.withScope {
    new SpatialKnnJoinRDD(self, other, k, distFunc)
  }

  override def zipJoin[V2: ClassTag](other: RDD[(G,V2)], pred: JoinPredicate.JoinPredicate) = {

    val predFunc = JoinPredicate.predicateFunction(pred)

    self.zipPartitions(other, preservesPartitioning = true){ (leftIter,rightIter) =>

      if(!leftIter.hasNext || !rightIter.hasNext) {
        Iterator.empty
      } else {

        val leftArray = leftIter.toArray

        rightIter.flatMap{ case(rg,rv) =>
          leftArray.iterator
            .filter { case (lg, _) =>
              predFunc(lg, rg)
            }
            .map{ case (_,lv) =>
              (lv,rv)
            }
        }
      }
    }.distinct()
  }

  /**
   * Cluster this SpatialRDD using DBSCAN
   *
   * @param minPts MinPts parameter to DBSCAN
   * @param epsilon Epsilon parameter to DBSCAN
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
		  ): RDD[(G, (Int, V))] = self.withScope{

	  // create a dbscan object with given parameters
	  val dbscan = new DBScan[KeyType,(G,V)](epsilon, minPts)
	  dbscan.maxPartitionSize = maxPartitionCost

	  // DBScan expects Vectors -> transform the geometry into a vector
	  val r = self.map{ case (g,v) =>
  	  /* TODO: can we make this work for polygons as well?
		   * See Generalized DBSCAN:
		   * Sander, Ester, Krieger, Xu
		   * "Density-Based Clustering in Spatial Databases: The Algorithm GDBSCAN and Its Applications"
		   * http://link.springer.com/article/10.1023%2FA%3A1009745219419
		   */
		  val c = g.getCentroid


		  /* extract the key for the cluster points
		   * this is used to distinguish points that have the same coordinates
		   */
		  val key = keyExtractor((g,v))

		  /* emit the tuple as input for the clustering
		   *  (id, coordinates, payload)
		   * The payload is the actual tuple to which we will project at the end
		   * to hide the internals of this implementation and to return data that
		   * matches the input format and
		   */

		  (key, Vectors.dense(c.getX, c.getY), (g,v))
	  }

	  // start the DBScan computation
	  val model = dbscan.run(r)

	  /*
	   * Finally, transform into a form that corresponds to a spatial RDD
	   * (Geo, (ClusterID, V)) - where V is the rest of the tuple, i.e. its
	   * actual content.
	   *
	   * Also, remove noise if desired.
	   * TODO: If noise has to be removed, can we use this more deeply inside
	   * the DBSCAN code to reduce data?
	   *
	   * We do know that there is a payload, hence calling .get is safe
	   */
    val points = if (includeNoise) model.points else model.points.filter(_.label != ClusterLabel.Noise)

    /* if the outfile is defined, write the clustering result
     * this can be used for debugging and visualization
     */
    if(outfile.isDefined)
      points.coalesce(1).saveAsTextFile(outfile.get)

    points.map { p => (p.payload.get._1, (p.clusterId, p.payload.get._2)) }
  }

  override def skyline(
      ref: STObject,
      distFunc: (STObject, STObject) => (Distance, Distance),
      dominates: (STObject, STObject) => Boolean,
      ppD: Int,
      allowCache: Boolean = false): RDD[(G,V)] = self.withScope{

    def localSkyline(iter: Iterator[(STObject,(G,V))]): Iterator[(STObject,(G,V))] = {

      val s = new Skyline[(G,V)](dominates = dominates)

      iter.foreach(tuple => s.insert(tuple))
      s.iterator
    }

    /*
     * compute the distance in space and time for each element
     * The STObject in first position represents the distances
     */
    val distanceRDD = self.map{ case (g,v) =>
      val (sDist, tDist) = distFunc(ref, g)

      (STObject(sDist.minValue, tDist.minValue), (g,v))
    }

    // TODO: specify parititoner as parameter - but it has to work on distance RDD...
    val partitioner = SpatialGridPartitioner(distanceRDD, ppD, pointsOnly = true)
    val partedDistRDD = distanceRDD.partitionBy(partitioner)

    val cachedDistRDD = if(allowCache) partedDistRDD.cache() else partedDistRDD

    val globalBS = cachedDistRDD.mapPartitionsWithIndex{ case (idx, iter) =>
      val bs = mutable.BitSet.empty
      if(iter.nonEmpty)
        bs += idx

      Iterator(bs)
    }.reduce{ case (bs1, bs2) => bs1 | bs2}

    for(idx <- globalBS) {
      val currMax = partitioner.partitionExtent(idx).ur.c
      val currMaxTup = STObject(currMax(0), currMax(1))

      // TODO: here we can be more selective by computing which partitions are dominated
      // based on the partiton index
      //FIXME is this ok, if dominateas eg wants maximum values?
      (idx until partitioner.numPartitions).foreach{ otherPart =>
        val otherMin = partitioner.partitionExtent(otherPart).ll.c
        val otherMinTup = STObject(otherMin(0), otherMin(1))


        if(Skyline.centroidDominates(currMaxTup, otherMinTup)) {
          globalBS(otherPart) = false
        }
      }
    }

    val bsBC = self.sparkContext.broadcast(globalBS)

    cachedDistRDD.mapPartitionsWithIndex{ case (idx, iter) =>
        if(bsBC.value(idx))
          localSkyline(iter)
        else
          Iterator.empty
      } // compute local skyline in each partition
      .coalesce(1, shuffle = true)                // collect local skyline points into a single partition
      .mapPartitions(localSkyline) // compute skyline out of the local skyline points
      .map(_._2)                  // transform back to input format

  }

  override def skylineAgg(ref: STObject,
                          distFunc: (STObject, STObject) => (Distance, Distance),
                          dominatesRel: (STObject, STObject) => Boolean
                ): RDD[(G,V)] = self.withScope{

    def combine(sky: Skyline[(G,V)], tuple: (G,V)): Skyline[(G,V)] = {
      val dist = distFunc(tuple._1, ref)
      val distObj = STObject(dist._1.minValue, dist._2.minValue)
      sky.insert((distObj, tuple))
      sky
    }

    def merge(sky1: Skyline[(G,V)], sky2: Skyline[(G,V)]): Skyline[(G,V)] = {
      sky1.merge(sky2)
    }


    val startSkyline = new Skyline[(G,V)](dominates = dominatesRel)
    val skyline = self.aggregate(startSkyline)(combine,merge)

    self.sparkContext.parallelize(skyline.skylinePoints.map(_._2))
  }

  def skylineAngular(ref: STObject,
                     distFunc: (STObject, STObject) => (Distance, Distance),
                     dominatesRel: (STObject, STObject) => Boolean,
                     ppd: Int): RDD[(G,V)] = self.withScope{

    require(ppd > 0)


    def combine(sky: Skyline[(G,V)], tuple: (STObject,(G,V))): Skyline[(G,V)] = {
      sky.insert(tuple)
      sky
    }

    def merge(sky1: Skyline[(G,V)], sky2: Skyline[(G,V)]): Skyline[(G,V)] = {
      sky1.merge(sky2)
    }


    val startSkyline = new Skyline[(G,V)](dominates = dominatesRel)

    val parti = new AngularPartitioner(2, ppd, true)
    val skyline = self.map{ case (g,v) =>
      val (sDist,tDist) = distFunc(ref, g)
      (STObject(sDist.minValue,tDist.minValue), (g,v))
    }.partitionBy(parti)
      .aggregate(startSkyline)(combine,merge)

    self.sparkContext.parallelize(skyline.skylinePoints.map(_._2))
  }

  def skylineAngularNoPart(ref: STObject,
                           distFunc: (STObject, STObject) => (Distance, Distance),
                           dominatesRel: (STObject, STObject) => Boolean,
                           ppd: Int): RDD[(G,V)] = {

    // for each partition (NOT necessarily spatial partition)
    self.mapPartitions{ iter =>

      val parti = new AngularPartitioner(dimensions = 2, ppD = ppd, firstQuadrantOnly = true)

      val externalMap = SpatialRDD.createExternalSkylineMap[G,V](dominatesRel)
      val values = iter.map{ case (g,v) =>
        val (sDist,tDist) = distFunc(ref, g)
        val distSO = STObject(sDist.minValue,tDist.minValue)
        val id = parti.getPartition(distSO) // assign each element to its partition,

        (id, (distSO,(g,v)))
      }
      /* and insert into an external map
       * during insert, each bucket is created by key and the value is the skyline for this partition id
       */
      externalMap.insertAll(values)
      externalMap.iterator



      /* Now, in the map each bucket corresponds to an (theoretical) angular partition and the value is the Skyline
       * This mapping exists for every physical partition, so in the next step, merge those partitions
       *
       * This can be done by reducing by key so that we merge all skyline with the same logical angular partition ID
       */
    }
    .reduceByKey{ (skyline1, skyline2) => skyline1.merge(skyline2) }
    .coalesce(1)
    .mapPartitions{ iter =>
      iter.map(_._2).reduce(_.merge(_)).iterator.map(_._2)
    }
  }

  // LIVE

  def liveIndex(order: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(None, RTreeConfig(order))

  def liveIndex(partitioner: SpatialPartitioner, order: Int): LiveIndexedSpatialRDDFunctions[G,V] =
    liveIndex(Some(partitioner), RTreeConfig(order))

  def liveIndex(indexConfig: IndexConfig): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(None, indexConfig)
  def liveIndex(partitioner: SpatialPartitioner, indexConfig: IndexConfig): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(Some(partitioner), indexConfig)

  def liveIndex(partitioner: Option[SpatialPartitioner], order: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(partitioner, RTreeConfig(order))

  def liveIndex(partitioner: Option[SpatialPartitioner], indexConfig: IndexConfig): LiveIndexedSpatialRDDFunctions[G,V] = {
    val reparted = if(partitioner.isDefined) self.partitionBy(partitioner.get) else self
    new LiveIndexedSpatialRDDFunctions(reparted, indexConfig)
  }

  def index(partitioner: SpatialPartitioner, order: Int): RDD[Index[(G,V)]] = index(Some(partitioner), RTreeConfig(order))

  /**
    * Create an index for each partition.
    *
    * This puts all data items of a partition into an Index structure, e.g., R-tree
    * and thus changes the type of the RDD from {{{ RDD[(STObject, V)]  }}} to
    * {{{ RDD[Index[STObject, (STObject, V)]] }}}
    */
  def index(partitioner: Option[SpatialPartitioner] = None, indexConfig: IndexConfig): RDD[Index[(G,V)]] = {
    val reparted = if(partitioner.isDefined) self.partitionBy(partitioner.get) else self

    reparted.mapPartitions({ iter  =>
      val tree = IndexFactory.get[(G,V)](indexConfig)

      iter.foreach{ case (g,v) => tree.insert(g, (g,v))}
      Iterator.single(tree)
    },
    preservesPartitioning = true) // preserve partitioning
  }

  def index(indexConfig: IndexConfig): RDD[Index[(G,V)]] = index(indexConfig, None)

  def index(indexConfig: IndexConfig,config: Option[PartitionerConfig]): RDD[Index[(G,V)]] = config match {
    case None =>
      val noParti: Option[SpatialPartitioner] = None
      index(noParti, indexConfig)
    case Some(c) => index(c, indexConfig)
  }

  def index(partitionerConfig: PartitionerConfig, indexConfig: IndexConfig): RDD[Index[(G,V)]] =
    index(PartitionerFactory.get(partitionerConfig, self), indexConfig)


  def rasterize[U : ClassTag](tileWidth: Int, pixelWidth: Double, globalUlx: Double, globalUly: Double,
                                            partitions: Int, converter: V => U)(implicit ord:Ordering[U]): RasterRDD[U] = {

    val parti = GridStrategy(tileWidth, pointsOnly = true, Some((-180,180,-90,90)), sampleFraction = 0)

    val parted = self.partitionBy(PartitionerFactory.get(parti,self).get)

    val tileHeight = tileWidth

    val width = 14400
    val height = 7200

    val numXTiles = width / tileWidth
    val numYTiles = height / tileHeight


    val tileLengthX = tileWidth * pixelWidth
    val tileLengthY = tileHeight * pixelWidth

    parted.mapPartitionsWithIndex((tileNum, iter) => {
      val arr = iter.map(t => converter(t._2)).toArray

      val xTile = tileNum % numXTiles
      val yTile = tileNum / numYTiles

      val ulx = -180 + xTile * tileLengthX
      val uly = 90 - yTile * tileLengthY

      Iterator.single(Tile(ulx, uly, tileWidth, tileWidth, arr, pixelWidth))
    }, preservesPartitioning = true).coalesce(partitions)
  }
}
