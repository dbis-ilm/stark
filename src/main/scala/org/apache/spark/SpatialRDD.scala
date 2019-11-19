package org.apache.spark

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.persistent.PersistedIndexedSpatialRDDFunctions
import dbis.stark.spatial.indexed.{Index, IndexConfig}
import dbis.stark.spatial.partitioner.SpatialPartitioner
import dbis.stark.spatial.{JoinPredicate, PredicatesFunctions, Skyline}
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalAppendOnlyMap

import scala.reflect.ClassTag


/**
 * A base class for spatial RDD without indexing
 *
 */
abstract class SpatialRDD[G <: STObject : ClassTag, V: ClassTag](
    @transient private val _sc: SparkContext,
    @transient private val _deps: Seq[Dependency[_]]
  ) extends RDD[(G,V)](_sc, _deps) {

  def this(@transient oneParent: RDD[(G,V)]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))

  override val partitioner = firstParent[(G,V)].partitioner

//  override protected def getPreferredLocations(split: Partition) =
//    firstParent[(G,V)].getPreferredLocations(split)

  /**
   * We do not repartition our data.
   */
  override protected def getPartitions: Array[Partition] = firstParent[(G,V)].partitions

//  def partitionBy(strategy: PartitionerConfig) = withScope {
//    val partitioner = PartitionerFactory.get(strategy, this)
//    firstParent[(G,V)].partitionBy(partitioner)
//  }
//    new PlainSpatialRDDFunctions(this).partitionBy(strategy)

  /**
   * Find all elements that are within a given radius around the given object
   */
  def withinDistance(
      qry: G, 
      maxDist: Distance,
      distFunc: (STObject,STObject) => Distance
    ) = withScope {
    new SpatialFilterRDD(this, qry, context.clean(PredicatesFunctions.withinDistance(maxDist, distFunc) _))
  }


//    new PlainSpatialRDDFunctions(this).withinDistance(qry, maxDist, distFunc)


  def filter(qry: G, pred: JoinPredicate, index: Option[IndexConfig] = None) = withScope {
    val f = JoinPredicate.predicateFunction(pred)
    val cleanF = _sc.clean(f)
    new SpatialFilterRDD(this, qry, pred, cleanF, index, checkParties = true)
  }

  /**
   * Compute an intersection of the elements in this RDD with the given geometry
   */
  //new PlainSpatialRDDFunctions(this).intersects(qry)
  def intersects(qry: G) =  withScope {
    val cleanF = _sc.clean(JoinPredicate.predicateFunction(JoinPredicate.INTERSECTS))

    new SpatialFilterRDD(this, qry, JoinPredicate.INTERSECTS, cleanF, None, checkParties = true)
  }

  /**
   * Find all elements that are contained by the given geometry
   *
   * @param qry The Geometry that should contains elements of this RDD
   * @return Returns an RDD containing the elements of this RDD that are completely contained by qry
   */
  def containedby(qry: G) = new PlainSpatialRDDFunctions(this).containedby(qry)

  /**
   * Find all elements that contain the given geometry.
   *
   * @param g The geometry that must be contained by other geometries in this RDD
   * @return Returns an RDD consisting of all elements in this RDD that contain the given geometry g
   */
  def contains(g: G) = new PlainSpatialRDDFunctions(this).contains(g)

  /**
   * Find the k nearest neighbors of the given geometry in this RDD
   *
   * @param qry The geometry to find the nearest neighbors of
   * @param k The number of nearest neighbors to find
   * @return Returns an RDD containing the k nearest neighbors of qry
   */
  def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))] = new PlainSpatialRDDFunctions(this).kNN(qry, k,distFunc)



}

/**
 * A helper companion object that contains implicit conversion methods to convert
 * simple RDDs to SpatialRDDs
 */
object SpatialRDD {

  def isSpatialParti(p: Option[Partitioner]) = p.flatMap {
    case _: SpatialPartitioner => Some(true)
    case _ => Some(false)
  }.getOrElse(false)

  def createExternalSkylineMap[G <: STObject : ClassTag, V : ClassTag](dominates: (STObject,STObject) => Boolean):
    ExternalAppendOnlyMap[Int, (STObject,(G,V)), Skyline[(G,V)]] = {

    type ValuePair = (G,V)
    type Combiner = Skyline[ValuePair]

    val createCombiner: ((STObject, ValuePair)) => Combiner = pair => {
      new Skyline(List((pair._1,pair._2)), dominates)
    }

    val mergeValue: (Combiner, (STObject, ValuePair)) => Combiner = (skyline, value) => {
      skyline.insert(value._1,value._2)
      skyline
    }

    val mergeCombiners: ( Combiner, Combiner ) => Combiner = (c1, c2) => {
      c1.merge(c2)
    }

    new ExternalAppendOnlyMap[Int, (STObject, ValuePair), Combiner](createCombiner, mergeValue, mergeCombiners)

  }


  def prepareForZipJoin[G <: STObject: ClassTag, V: ClassTag, V2: ClassTag](leftRDD: RDD[(G,V)], rightRDD: RDD[(G,V2)],
                                                                            partitioner: SpatialPartitioner) = {

    val hashPartitioner = new org.apache.spark.HashPartitioner(partitioner.numPartitions)
    val sc = leftRDD.sparkContext

    val partiBc = sc.broadcast(partitioner)

    val leftPrepared = leftRDD.mapPartitions({iter =>
      val theParti = partiBc.value
      iter.flatMap { case (so, v) =>
        theParti.getIntersectingPartitionIds(so).map(idx => (idx, (so,v)))
      }
    }, preservesPartitioning = true)
      .partitionBy(hashPartitioner)
      .mapPartitions({ iter => iter.map{ case (_,t) => t}}, preservesPartitioning = true)

    val rightPrepared = rightRDD.mapPartitions({iter =>
      val theParti = partiBc.value
      iter.flatMap { case (so, v) =>
        theParti.getIntersectingPartitionIds(so).map(idx => (idx, (so,v)))
      }
    }, preservesPartitioning = true)
      .partitionBy(hashPartitioner)
      .mapPartitions({ iter => iter.map{ case (_,t) => t}}, preservesPartitioning = true)

    (leftPrepared, rightPrepared)
  }
  
  /**
   * Convert an RDD to a "plain" spatial RDD which uses no indexing.
   *
   * @param rdd The RDD to convert
   * @return Returns a SpatialRDDFunctions object that contains spatial methods
   */
	implicit def convertSpatialPlain[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G, V)]): PlainSpatialRDDFunctions[G, V]
    = new PlainSpatialRDDFunctions[G,V](rdd)

	/**
	 * Convert an RDD to a SpatialRDD which uses persisted indexing.
	 *
	 * @param rdd The RDD to convert
	 * @return Returns a IndexedSpatialRDDFunctions object that contains spatial methods that use indexing
	 */
	implicit def convertSpatialPersistedIndexing[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[Index[(G,V)]]): PersistedIndexedSpatialRDDFunctions[G, V]
    = new PersistedIndexedSpatialRDDFunctions(rdd)

}


