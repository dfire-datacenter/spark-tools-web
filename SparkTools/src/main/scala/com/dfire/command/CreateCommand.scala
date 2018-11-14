package com.dfire.command

import com.dfire.command.utils.{SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Description :
  * Author ： HeGuoZi
  * Date ： 10:20 2018/11/14
  * Modified :
  */
/**
  * Create a table and optionally insert some data into it. Note that this plan is unresolved and
  * has to be replaced by the concrete implementations during analysis.
  *
  * @param tableDesc the metadata of the table to be created.
  * @param mode      the data writing mode
  * @param query     an optional logical plan representing data to write into the created table.
  */
case class CreateTable(
                        tableDesc: CatalogTable,
                        mode: SaveMode,
                        query: Option[LogicalPlan]) extends LogicalPlan {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def children: Seq[LogicalPlan] = query.toSeq

  override def output: Seq[Attribute] = Seq.empty

  override lazy val resolved: Boolean = false
}

/**
  * A command to create a table.
  *
  * Note: This is currently used only for creating Hive tables.
  * This is not intended for temporary tables.
  *
  * The syntax of using this command in SQL is:
  * {{{
  *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
  *   [(col1 data_type [COMMENT col_comment], ...)]
  *   [COMMENT table_comment]
  *   [PARTITIONED BY (col3 data_type [COMMENT col_comment], ...)]
  *   [CLUSTERED BY (col1, ...) [SORTED BY (col1 [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  *   [SKEWED BY (col1, col2, ...) ON ((col_value, col_value, ...), ...)
  *   [STORED AS DIRECTORIES]
  *   [ROW FORMAT row_format]
  *   [STORED AS file_format | STORED BY storage_handler_class [WITH SERDEPROPERTIES (...)]]
  *   [LOCATION path]
  *   [TBLPROPERTIES (property_name=property_value, ...)]
  *   [AS select_statement];
  * }}}
  */
case class CreateTableCommand(
                               table: CatalogTable,
                               ignoreIfExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }
}