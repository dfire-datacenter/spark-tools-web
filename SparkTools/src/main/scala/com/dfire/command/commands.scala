package com.dfire.command

import com.dfire.command.utils.{SQLMetric, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types._

/**
  * A logical command that is executed for its side-effects.  `RunnableCommand`s are
  * wrapped in `ExecutedCommand` during execution.
  */
trait RunnableCommand extends Command {

  // The map used to record the metrics of running the command. This will be passed to
  // `ExecutedCommand` during query planning.
  lazy val metrics: Map[String, SQLMetric] = Map.empty

  def run(sparkSession: SparkSession): Seq[Row]
}

/**
  * An explain command for users to see how a command will be executed.
  *
  * Note that this command takes in a logical plan, runs the optimizer on the logical plan
  * (but do NOT actually execute it).
  *
  * {{{
  *   EXPLAIN (EXTENDED | CODEGEN) SELECT * FROM ...
  * }}}
  *
  * @param logicalPlan plan to explain
  * @param extended    whether to do extended explain or not
  * @param codegen     whether to output generated code from whole-stage codegen or not
  * @param cost        whether to show cost information for operators.
  */
case class ExplainCommand(
                           logicalPlan: LogicalPlan,
                           extended: Boolean = false,
                           codegen: Boolean = false,
                           cost: Boolean = false)
  extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val outputString = ""
    Seq(Row(outputString))
  } catch {
    case cause: TreeNodeException[_] =>
      ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}
