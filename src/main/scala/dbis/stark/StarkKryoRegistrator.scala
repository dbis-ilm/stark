package dbis.stark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.locationtech.jts.geom.{Geometry, Point, Polygon}

class StarkKryoRegistrator extends KryoRegistrator {


  private val classes: Seq[Class[_]] = Seq(
    classOf[Instant], classOf[Interval],
    classOf[Geometry], classOf[Point], classOf[Polygon],
    classOf[STObject]
  )

  override def registerClasses(kryo: Kryo): Unit = classes.foreach(clazz => kryo.register(clazz))
}
