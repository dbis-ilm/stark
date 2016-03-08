package dbis.spark.spatial.indexed.persistent

import org.apache.spark.rdd.CoGroupedRDD
import dbis.spark.spatial.SpatialRDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.CoGroupPartition
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import dbis.spark.spatial.indexed.live.IndexedSpatialRDD
import dbis.spark.spatial.indexed.RTree
import dbis.spark.spatial.indexed.IndexedPartition
import java.io.IOException
import java.io.ObjectOutputStream
import dbis.spark.spatial.SpatialPartitioner
import org.apache.spark.Dependency
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import scala.collection.mutable.ListBuffer
import dbis.spark.spatial.SpatialGridPartitioner
import dbis.spark.spatial.BSPartitioner


protected[spatial] case class NarrowIndexJoinSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = { 
      // Update the reference to parent split at the time of task serialization
      split = rdd.partitions(splitIndex)
      oos.defaultWriteObject()
  }
}

protected[spatial] class JoinPartition(
    idx: Int, 
    val leftDep: NarrowIndexJoinSplitDep,
    val rightDeps: Array[NarrowIndexJoinSplitDep]
  ) extends Partition with Serializable {
  
  override def index = idx
  override def hashCode() = idx 
}

class IndexedSpatialJoinRDD[G <: Geometry : ClassTag, V: ClassTag, V2: ClassTag](
    @transient val left: RDD[RTree[G, (G,V)]], //IndexedSpatialRDD[G,V] 
    @transient val right: RDD[(G,V2)]
//    part: SpatialPartitioner
  )  extends SpatialRDD[G,Iterable[(V,V2)]](left.context, Nil) /*CoGroupedRDD(Seq(left, right), left.partitioner.get)*/ {
  
  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(left),
    new OneToOneDependency(right)
  ) 
  
  override def getPartitions = Array.tabulate(left.getNumPartitions){ i =>
    new JoinPartition(i, 
      new NarrowIndexJoinSplitDep(left, i, left.partitions(i)),
      Array.tabulate(right.getNumPartitions)(j => new NarrowIndexJoinSplitDep(right, j, right.partitions(j))))
  }
  
  override def compute(s: Partition, context: TaskContext): Iterator[(G,Iterable[(V,V2)])] = {
    val split = s.asInstanceOf[JoinPartition]
    
    val left  = split.leftDep.rdd.iterator(split.leftDep.split, context).asInstanceOf[Iterator[RTree[G, (G,V)]]].toList
    
    /* FIXME use external map for join
     * eigentlich muesste der Combiner einen R-Baum haben 
     * aber das funktioneirt wahrscheinlich nicht mehr mit der Map --> was eigenes implementieren
     */
    
    require(left.size == 1, "left should be only one partition (for index)")
    
    val map = createExternalMap

    val idx = left.head

   
    for(rDep <- split.rightDeps) {
      val right = rDep.rdd.iterator(rDep.split, context).asInstanceOf[Iterator[(G,V2)]]

      val res = right.flatMap{ case (rg, rv) =>
  		  idx.query(rg) // query R-Tree and get matching MBBs
      		  .filter{ case (lg, _) => lg.intersects(rg)} // check if real geom also matches
      		  .map { case (lg, lv) => (lg,(lv, rv))  }    // key is the left geom
      }
      
      map.insertAll(res)
    }
    
    
    new InterruptibleIterator(context, map.iterator)
  }
  
  type ValuePair = (V,V2)
  type Combiner = ListBuffer[ValuePair]
  
  private def createExternalMap(): ExternalAppendOnlyMap[G, ValuePair, Combiner] = {
    val createCombiner: ( ValuePair => Combiner) = pair => ListBuffer(pair)
    
    val mergeValue: (Combiner, ValuePair) => Combiner = (list, value) => list += value
    
    val mergeCombiners: ( Combiner, Combiner ) => Combiner = (c1, c2) => c1 ++= c2
    
    new ExternalAppendOnlyMap[G, ValuePair, Combiner](createCombiner, mergeValue, mergeCombiners)
    
  }  
}

