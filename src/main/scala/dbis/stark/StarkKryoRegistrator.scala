package dbis.stark

import com.esotericsoftware.kryo.Kryo
import dbis.stark.raster.{SMA, Tile}
import dbis.stark.spatial._
import dbis.stark.spatial.partitioner.{CellHistogram, OneToManyPartition}
import org.apache.spark.serializer.KryoRegistrator
import org.locationtech.jts.index.strtree.RTree
import org.locationtech.jts.geom._

class StarkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    ////    kryo.setReferences(true)
    ////    kryo.setCopyReferences(true)
    //
    val temporalSerializer = new TemporalSerializer()
    kryo.register(classOf[Instant], temporalSerializer)
    kryo.register(classOf[Interval], temporalSerializer)

    val geometrySerializer = new GeometrySerializer()

    kryo.register(classOf[Point], geometrySerializer)
    kryo.register(classOf[Polygon], geometrySerializer)
    kryo.register(classOf[LineString], geometrySerializer)
    kryo.register(classOf[Rectangle], geometrySerializer)
    kryo.register(classOf[MultiPolygon], geometrySerializer)

    kryo.register(classOf[Envelope], new EnvelopeSerializer())
    kryo.register(classOf[STObject], new STObjectSerializer)

    kryo.register(classOf[RTree[_]], new RTreeSerializer)

    kryo.register(classOf[OneToManyPartition], new OneToMayPartitionSerializerBuffer)

    kryo.register(classOf[NPoint], new NPointSerializer())
    kryo.register(classOf[NRectRange], new NRectSerializer())

    kryo.register(classOf[Cell], new CellSerializer)
    kryo.register(classOf[CellHistogram], new HistogramSerializer)

    //    val distanceSerializer = new DistanceSerializer
    //    kryo.register(classOf[ScalarDistance], distanceSerializer)
    //    kryo.register(classOf[IntervalDistance], distanceSerializer)

    kryo.register(classOf[KNN[_]], new KnnSerializer)

    kryo.register(classOf[Skyline[_]], new SkylineSerializer)

    kryo.register(classOf[(STObject, Byte)], new PairSerializerBuffer)

    kryo.register(classOf[Tile[_]])
    kryo.register(classOf[SMA[_]])

  }
}
