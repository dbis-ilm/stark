package dbis.stark.spatial

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.persistent.PersistedIndexedSpatialRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalAppendOnlyMap

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * A helper companion object that contains implicit conversion methods to convert
 * simple RDDs to SpatialRDDs
 */
object SpatialRDD {

  protected[stark] def createExternalMap[G : ClassTag, V : ClassTag, V2: ClassTag](): ExternalAppendOnlyMap[G, (V,V2), ListBuffer[(V,V2)]] = {
    
    type ValuePair = (V,V2)
    type Combiner = ListBuffer[ValuePair]
    
    val createCombiner: ( ValuePair => Combiner) = pair => ListBuffer(pair)
    
    val mergeValue: (Combiner, ValuePair) => Combiner = (list, value) => list += value
    
    val mergeCombiners: ( Combiner, Combiner ) => Combiner = (c1, c2) => c1 ++= c2
    
    new ExternalAppendOnlyMap[G, ValuePair, Combiner](createCombiner, mergeValue, mergeCombiners)
    
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
	implicit def convertSpatialPersistedIndexing[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[RTree[G,(G,V)]]): PersistedIndexedSpatialRDDFunctions[G, V]
    = new PersistedIndexedSpatialRDDFunctions(rdd)

}


