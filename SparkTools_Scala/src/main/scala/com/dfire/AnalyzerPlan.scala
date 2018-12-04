package com.dfire

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CASE_SENSITIVE, GROUP_BY_ORDINAL}
import org.apache.spark.sql.util.Logging

/**
  * Description : AnalyzerPlan
  * Author ： HeGuoZi
  * Date ： 16:21 2018/11/29
  * Modified :
  */
class AnalyzerPlan  extends Serializable with Logging {

  def analyze():String ={
    val conf = new SQLConf().copy(CASE_SENSITIVE -> false, GROUP_BY_ORDINAL -> true)
    val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
    val analyzer = new Analyzer(catalog, conf)
    ""
  }

}
