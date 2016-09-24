package dbis.stark.spatial.indexed.live


import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD
import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.plain.CartesianPartition
import dbis.stark.spatial.SpatialRDD


private[stark] class LiveIndexedSpatialCartesianJoinRDD[G <: STObject : ClassTag, G2 <: STObject: ClassTag, V: ClassTag, V2: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[(G,V)],
    var rdd2 : RDD[(G2,V2)],
    predicate: (G,G2) => Boolean,
    capacity: Int)
  extends RDD[(V, V2)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V, V2)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
  
    val tree = new RTree[G,(G,V)](capacity)
    
    rdd1.iterator(currSplit.s1, context).foreach{ case (g,v) => tree.insert(g,(g,v))}
    
    tree.build()
    
    val map = SpatialRDD.createExternalMap[G,V,V2]
    
    rdd2.iterator(currSplit.s2, context).foreach{ case (rg,rv) =>
      tree.query(rg)
          .filter { case (lg,_) => predicate(lg,rg)}
          .map{ case (lg,lv) => (lg,(lv,rv)) }
          .foreach { case (g,v) => map.insert(g,v) }
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
