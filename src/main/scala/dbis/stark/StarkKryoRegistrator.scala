package dbis.stark

import com.esotericsoftware.kryo.Kryo
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.{NPoint, NRectRange}
import org.apache.spark.serializer.KryoRegistrator
import org.locationtech.jts.geom._

class StarkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    val temporalSerializer = new TemporalSerializer()

    kryo.register(classOf[Instant], temporalSerializer)
    kryo.register(classOf[Interval], temporalSerializer)

    val geometrySerializer = new GeometrySerializer()

//    kryo.register(classOf[Geometry]) // use Kryo's default serializer for everything else -- does this work?

    kryo.register(classOf[Point], geometrySerializer)
    kryo.register(classOf[Polygon], geometrySerializer)
    kryo.register(classOf[LineString], geometrySerializer)
    kryo.register(classOf[Envelope], new EnvelopeSerializer())

    kryo.register(classOf[STObject], new STObjectSerializer())

    kryo.register(classOf[(STObject,Any)], new StarkSerializer)


    kryo.register(classOf[NPoint], new NPointSerializer())
    kryo.register(classOf[NRectRange], new NRectSerializer())

    kryo.register(classOf[RTree[_]], new RTreeSerializer)

  }
}