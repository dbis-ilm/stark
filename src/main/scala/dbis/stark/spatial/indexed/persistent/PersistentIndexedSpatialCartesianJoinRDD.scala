package dbis.stark.spatial.indexed.persistent


import dbis.stark.STObject
import dbis.stark.STObject.MBR
import dbis.stark.spatial.indexed.{Index, RTree}
import dbis.stark.spatial.partitioner.{JoinPartition, SpatialPartitioner}
import dbis.stark.spatial.{SpatialRDD, Utils}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


private[stark] class PersistentIndexedSpatialCartesianJoinRDD[G <: STObject : ClassTag, G2 <: STObject: ClassTag, V: ClassTag, V2: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[Index[G,(G,V)]],
    var rdd2 : RDD[(G2,V2)],
    predicate: (G,G2) => Boolean)
  extends RDD[(V, V2)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  private lazy val rightParti = {
    val p = rdd2.partitioner 
      if(p.isDefined) {
        p.get match {
          case sp: SpatialPartitioner => Some(sp)
          case _ => None
        }
      } else 
        None
  }
  
  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new JoinPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[JoinPartition]
    (rdd1.preferredLocations(currSplit.leftPartition) ++ rdd2.preferredLocations(currSplit.rightPartition)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V, V2)] = {
    val currSplit = split.asInstanceOf[JoinPartition]

    val map = SpatialRDD.createExternalMap[G, V, V2]()
    
    rdd1.iterator(currSplit.leftPartition, context).foreach{ tree =>
      
      /*
       * Returns:
  		 * an Envelope (for STRtrees), an Interval (for SIRtrees), or other object 
  		 * (for other subclasses of AbstractSTRtree)
  		 * 
  		 * http://www.atetric.com/atetric/javadoc/com.vividsolutions/jts-core/1.14.0/com/vividsolutions/jts/index/strtree/Boundable.html#getBounds--
       */

      require(tree.isInstanceOf[RTree[G,(G,V)]], s"persistent join only supported for R-Trees currently, but is: ${tree.getClass}")

      val indexBounds = tree.asInstanceOf[RTree[G,(G,V)]].root().getBounds.asInstanceOf[MBR]
      
      val partitionCheck = rightParti.forall { p =>
        indexBounds.intersects(Utils.toEnvelope(p.partitionExtent(currSplit.rightPartition.index)))
      }
      
      if(partitionCheck) {
        rdd2.iterator(currSplit.rightPartition, context).foreach{ case (rg,rv) =>
          tree.query(rg)
              .filter { case (lg,_) => predicate(lg,rg)}
              .map{ case (lg,lv) => (lg,(lv,rv)) }
              .foreach { case (g,v) => map.insert(g,v) }
        }
      }
    }    
    
    val f = map.iterator.flatMap{ case (g, l) => l}
    
    new InterruptibleIterator(context, f)
    
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
