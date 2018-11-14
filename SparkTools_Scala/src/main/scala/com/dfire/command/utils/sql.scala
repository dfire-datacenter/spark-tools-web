package com.dfire.command

import com.dfire.command.utils.SparkStrategy
import org.apache.spark.annotation.{DeveloperApi, InterfaceStability}

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 *  @groupname dataType Data types
 *  @groupdesc Spark SQL data types.
 *  @groupprio dataType -3
 *  @groupname field Field
 *  @groupprio field -2
 *  @groupname row Row
 *  @groupprio row -1
 */
package object sql {

  @DeveloperApi
  @InterfaceStability.Unstable
  type Strategy = SparkStrategy

}
