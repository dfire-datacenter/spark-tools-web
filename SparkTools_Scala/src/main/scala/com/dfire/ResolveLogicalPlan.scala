package com.dfire

import java.util

import com.dfire.command._
import org.apache.spark.Dependency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.util.Logging

import scala.collection.JavaConversions._

class ResolveLogicalPlan extends Serializable with Logging {

  case class DcTable(dataBase: String, tableName: String)

  def resolvePlan(plan: LogicalPlan, currentDB: String)
  : (util.Set[DcTable], util.Set[DcTable]) = {
    val inputTables = new util.HashSet[DcTable]()
    val outputTables = new util.HashSet[DcTable]()
    resolveLogic(plan, currentDB, inputTables, outputTables)
    Tuple2(inputTables, outputTables)
  }

  def resolveLogic(plan: LogicalPlan, currentDB: String,
                   inputTables: util.Set[DcTable],
                   outputTables: util.Set[DcTable]): Unit = {
    plan match {

      case plan: Project =>
        val project = plan.asInstanceOf[Project]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Union =>
        val project = plan.asInstanceOf[Union]
        for (child <- project.children) {
          resolveLogic(child, currentDB, inputTables, outputTables)
        }

      case plan: Join =>
        val project = plan.asInstanceOf[Join]
        resolveLogic(project.left, currentDB, inputTables, outputTables)
        resolveLogic(project.right, currentDB, inputTables, outputTables)

      case plan: Aggregate =>
        val project = plan.asInstanceOf[Aggregate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Filter =>
        val project = plan.asInstanceOf[Filter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Generate =>
        val project = plan.asInstanceOf[Generate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: RepartitionByExpression =>
        val project = plan.asInstanceOf[RepartitionByExpression]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SerializeFromObject =>
        val project = plan.asInstanceOf[SerializeFromObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapPartitions =>
        val project = plan.asInstanceOf[MapPartitions]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: DeserializeToObject =>
        val project = plan.asInstanceOf[DeserializeToObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Repartition =>
        val project = plan.asInstanceOf[Repartition]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Deduplicate =>
        val project = plan.asInstanceOf[Deduplicate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Window =>
        val project = plan.asInstanceOf[Window]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapElements =>
        val project = plan.asInstanceOf[MapElements]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: TypedFilter =>
        val project = plan.asInstanceOf[TypedFilter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Distinct =>
        val project = plan.asInstanceOf[Distinct]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SubqueryAlias =>
        val project = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new util.HashSet[DcTable]()
        val childOutputTables = new util.HashSet[DcTable]()

        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
        if (childInputTables.size() > 0) {
          for (table <- childInputTables) inputTables.add(table)
        } else {
          inputTables.add(DcTable(currentDB, project.alias))
        }

      case plan: HiveTableRelation =>
        val project = plan.asInstanceOf[HiveTableRelation]
        val identifier = project.tableMeta.identifier
        val dcTable = DcTable(identifier.database.getOrElse(currentDB), identifier.table)
        inputTables.add(dcTable)

      case plan: UnresolvedCatalogRelation =>
        val project = plan.asInstanceOf[UnresolvedCatalogRelation]
        val identifier = project.tableMeta.identifier
        val dcTable = DcTable(identifier.database.getOrElse(currentDB), identifier.table)
        inputTables.add(dcTable)

      case plan: UnresolvedRelation =>
        val project = plan.asInstanceOf[UnresolvedRelation]
        val dcTable = DcTable(project.tableIdentifier.database.getOrElse(currentDB),
          project.tableIdentifier.table)
        inputTables.add(dcTable)

      case plan: InsertIntoTable =>
        val project = plan.asInstanceOf[InsertIntoTable]
        resolveLogic(project.table, currentDB, outputTables, inputTables)
        resolveLogic(project.query, currentDB, inputTables, outputTables)

      case plan: InsertIntoHiveTable =>
        val project = plan.asInstanceOf[InsertIntoHiveTable]
        log.error("CatalogTable:" + project.table.database + project.table.identifier.table)
        val dcTable = DcTable(project.table.database,
          project.table.identifier.table)
        inputTables.add(dcTable)
        resolveLogic(project.query, currentDB, inputTables, outputTables)

      case plan: CreateTable =>
        val project = plan.asInstanceOf[CreateTable]
        if (project.query.isDefined) {
          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(dcTable)

      case plan: CreateTableCommand =>
        val project = plan.asInstanceOf[CreateTableCommand]
        if (project.ignoreIfExists) {
          // 此处可能会出现问题，project.children.head未必为当前plan的下一深度的叶子节点
          log.error("This point may occur potential problem " +
            "while project.children.head may not be the next-depth-child of current plan:\n"
            + project.children.toList.toString())
          resolveLogic(project.children.head, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.table.identifier
        val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(dcTable)

      case plan: CreateHiveTableAsSelectCommand =>
        val project = plan.asInstanceOf[CreateHiveTableAsSelectCommand]
        if (project.query != null) {
          resolveLogic(project.query, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB),
          tableIdentifier.table)
        outputTables.add(dcTable)

      case plan: GlobalLimit =>
        val project = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: LocalLimit =>
        val project = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Sort =>
        val project = plan.asInstanceOf[Sort]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case `plan` => log.error("******child_plan******:\n" + plan)
    }
  }

  def checkRddRelationShip(rdd1: RDD[_], rdd2: RDD[_]): Boolean = {
    if (rdd1.id == rdd2.id) return true
    dfsSearch(rdd1, rdd2.dependencies)
  }

  def dfsSearch(rdd1: RDD[_], dependencies: Seq[Dependency[_]]): Boolean = {
    for (dependency <- dependencies) {
      if (dependency.rdd.id == rdd1.id) return true
      if (dfsSearch(rdd1, dependency.rdd.dependencies)) return true
    }
    false
  }

}
