package com.dfire.command

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.language.existentials

/**
  * Command for writing the results of `query` to file system.
  *
  * The syntax of using this command in SQL is:
  * {{{
  *   INSERT OVERWRITE [LOCAL] DIRECTORY
  *   path
  *   [ROW FORMAT row_format]
  *   [STORED AS file_format]
  *   SELECT ...
  * }}}
  *
  * @param isLocal   whether the path specified in `storage` is a local directory
  * @param storage   storage format used to describe how the query result is stored.
  * @param query     the logical plan representing data to write to
  * @param overwrite whether overwrites existing directory
  */
case class InsertIntoHiveDirCommand(
                                     isLocal: Boolean,
                                     storage: CatalogStorageFormat,
                                     query: LogicalPlan,
                                     overwrite: Boolean,
                                     outputColumns: Seq[Attribute]) extends LogicalPlan {

  override def children: Seq[LogicalPlan] = query :: Nil

  override def output: Seq[Attribute] = Seq.empty

}

