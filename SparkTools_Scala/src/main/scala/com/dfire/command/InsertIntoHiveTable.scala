package com.dfire.command

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


/**
  * Command for writing data out to a Hive table.
  *
  * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
  * follow Hive's internal implementations closely, which itself was a mess too). Please don't
  * blame Reynold for this! He was just moving code around!
  *
  * In the future we should converge the write path for Hive with the normal data source write path,
  * as defined in `org.apache.spark.sql.execution.datasources.FileFormatWriter`.
  *
  * @param table                the metadata of the table.
  * @param partition            a map from the partition key to the partition value (optional). If the partition
  *                             value is optional, dynamic partition insert will be performed.
  *                             As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS ...` would have
  *
  *                             {{{
  *                                                                                                                                                                   Map('a' -> Some('1'), 'b' -> Some('2'))
  *                             }}}
  *
  *                             and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
  *                             would have
  *
  *                             {{{
  *                                                                                                                                                                   Map('a' -> Some('1'), 'b' -> None)
  *                             }}}.
  * @param query                the logical plan representing data to write to.
  * @param overwrite            overwrite existing table or partitions.
  * @param ifPartitionNotExists If true, only write if the partition does not exist.
  *                             Only valid for static partitions.
  */
case class InsertIntoHiveTable(
                                table: CatalogTable,
                                partition: Map[String, Option[String]],
                                query: LogicalPlan,
                                overwrite: Boolean,
                                ifPartitionNotExists: Boolean,
                                outputColumns: Seq[Attribute]) extends LogicalPlan {

  // We don't want `table` in children as sometimes we don't want to transform it.
  override def children: Seq[LogicalPlan] = query :: Nil

  override def output: Seq[Attribute] = Seq.empty
}
