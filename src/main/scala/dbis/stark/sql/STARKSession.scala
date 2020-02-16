package dbis.stark.sql

import dbis.stark.spatial.STSparkContext
import org.apache.spark.sql.SparkSession

/**
  An session instance for convenient enabling of STARK SQL features
 */
object STARKSession {

  /**
    * Builder class to construct STARK session
    */
  class STARKSessionBuilder() extends SparkSession.Builder {

    // flag to indicate STARK support
    private var enableSTARK: Boolean = false

    def enableSTARKSupport(): SparkSession.Builder = {
      enableSTARK = true

      withExtensions{ extensions =>
        extensions.injectPlannerStrategy(_ => StarkStrategy)
      }

      this
    }

//    def disableSTARKSupport(): SparkSession.Builder = {
//      enableSTARK = false
//      this
//    }

    /**
      * Get (or create) the SparkSession and automatically enable
      * STARK support if desired
      * @return Returns a SparkSession with enabled STARK support if desired
      */
    override def getOrCreate() = {
      val spark = super.getOrCreate()

      if(enableSTARK) {
        logInfo("enabling STARK SQL")
//        spark.experimental.extraStrategies = STJoinStrategy :: Nil
        dbis.stark.sql.Functions.register(spark)
      }

      spark
    }
  }

  /**
    * Get a builder instance with which a STARK enabled session can
    * be created conveniently
    * @return Returns a Session builder
    */
  def builder(): STARKSessionBuilder = {
    val builder= new STARKSessionBuilder()
    builder.enableSTARKSupport()
    builder
  }

  def fromContext(ctx: STSparkContext): SparkSession =
    SparkSession.builder.config(ctx.getConf).getOrCreate()

//  /**
//    * (implicit) conversion of a traiditonal SparkSession builder into a STARK Session builder
//    * @param sparkSessionBuilder The SparkSesson builder to use in STARKSession builder
//    * @return Returns a STARK Session builder
//    */
//  implicit def toSTARKSession(sparkSessionBuilder: SparkSession.Builder) = {
//
//    val b = new STARKSessionBuilder()
//    sparkSessionBuilder.
//  }
}
