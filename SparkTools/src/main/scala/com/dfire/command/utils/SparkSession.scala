package com.dfire.command.utils

import java.io.Closeable

import com.sun.deploy.util.SessionState
import org.apache.spark.SparkContext
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging


/**
  * The entry point to programming Spark with the Dataset and DataFrame API.
  *
  * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder
  * to get an existing session:
  *
  * {{{
  *   SparkSession.builder().getOrCreate()
  * }}}
  *
  * The builder can also be used to create a new session:
  *
  * {{{
  *   SparkSession.builder
  *     .master("local")
  *     .appName("Word Count")
  *     .config("spark.some.config.option", "some-value")
  *     .getOrCreate()
  * }}}
  *
  * @param sparkContext        The Spark context associated with this Spark session.
  * @param existingSharedState If supplied, use the existing shared state
  *                            instead of creating a new one.
  * @param parentSessionState  If supplied, inherit all session state (i.e. temporary
  *                            views, SQL config, UDFs etc) from parent.
  */
@InterfaceStability.Stable
class SparkSession private(
                            @transient val sparkContext: SparkContext,
                            @transient private val existingSharedState: Option[SharedState],
                            @transient private val parentSessionState: Option[SessionState],
                            @transient private[dfire] val extensions: SparkSessionExtensions)
  extends Serializable with Closeable with Logging {
  self =>

  /**
    * Stop the underlying `SparkContext`.
    *
    * @since 2.0.0
    */
  def stop(): Unit = {
    sparkContext.stop()
  }

  /**
    * Synonym for `stop()`.
    *
    * @since 2.1.0
    */
  override def close(): Unit = stop()

}


@InterfaceStability.Stable
object SparkSession extends Logging {

  /**
    * Builder for [[SparkSession]].
    */
  @InterfaceStability.Stable
  class Builder extends Logging {
  }

}
