package com.dfire.command.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.QueryPlan

abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

}