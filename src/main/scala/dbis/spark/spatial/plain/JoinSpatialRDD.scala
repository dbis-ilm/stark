package dbis.spark.spatial.plain

import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import dbis.spark.spatial.SpatialRDD
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import dbis.spark.spatial.indexed.persistent.JoinPartition
import dbis.spark.spatial.indexed.persistent.NarrowIndexJoinSplitDep
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.InterruptibleIterator

class JoinSpatialRDD[G <: Geometry : ClassTag, V: ClassTag, V2: ClassTag](
    @transient val left: RDD[(G,V)], 
    @transient val right: RDD[(G,V2)]
//    part: SpatialPartitioner
  )  extends SpatialRDD[G,(V,V2)](left.context, Seq(new OneToOneDependency(left), 
                                                              new OneToOneDependency(right))) {
  
  override def getPartitions = Array.tabulate(left.getNumPartitions){ i =>  new JoinPartition(i, 
		  new NarrowIndexJoinSplitDep(left, i, left.partitions(i)),
			  Array.tabulate(right.getNumPartitions){j => 
	  		  new NarrowIndexJoinSplitDep(right, j, right.partitions(j))
    	  }
		  )
  }
  
  @DeveloperApi
  override def compute(s: Partition, context: TaskContext): Iterator[(G,(V,V2))] = {
    val split = s.asInstanceOf[JoinPartition]
    val left  = split.leftDep.rdd.iterator(split.leftDep.split, context).asInstanceOf[Iterator[(G,V)]].toList

    /* FIXME use external map for join
     * eigentlich muesste der Combiner einen R-Baum haben 
     * aber das funktioneirt wahrscheinlich nicht mehr mit der Map --> was eigenes implementieren
     */
    
    
    val map = createExternalMap
    
    for((lg,lv) <- left) {
      val res = split.rightDeps.flatMap( d => d.rdd.iterator(d.split, context).asInstanceOf[Iterator[(G,V2)]])
                  .filter { case (rg, rv) => lg.intersects(rg) }
                  .map { case (rg, rv) => (lg,(lv, rv)) }
      
  		map.insertAll(res)
      
    }
    
    val f = map.iterator.flatMap{ case (g, l) => l.map { case (lv, rv) => (g,(lv,rv)) } }
    
    new InterruptibleIterator(context, f)
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