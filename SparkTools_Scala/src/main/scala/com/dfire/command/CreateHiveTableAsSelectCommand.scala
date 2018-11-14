package com.dfire.command

import com.dfire.command.utils.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


/**
  * Create table and insert the query result into it.
  *
  * @param tableDesc the Table Describe, which may contains serde, storage handler etc.
  * @param query     the query whose result will be insert into the new relation
  * @param mode      SaveMode
  */
case class CreateHiveTableAsSelectCommand(
                                           tableDesc: CatalogTable,
                                           query: LogicalPlan,
                                           outputColumns: Seq[Attribute],
                                           mode: SaveMode)
  extends LogicalPlan {

  override final def children: Seq[LogicalPlan] = query :: Nil

  override def output: Seq[Attribute] = Seq.empty

}
