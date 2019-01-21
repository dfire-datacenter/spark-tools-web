# spark-tools-web

sprak 相关工具类(如血缘解析)，提供http接口。

##1、部署环境

JDK1.8、Neo4j(推荐3.5.1版本，community)、Hive 1.1.0+。

##2、环境配置与部署

配置app.properties:

meta.jdbc：存储Hive MetaStore中的表和字段信息

neo4j.jdbc：Neo4j相关信息

hive.metastore：Hive MetaStore相关信息

spark：Spark Thriftserver信息，暂时可以忽略

lineage.mysql：存储血缘结果的库

hera.mysql：调度系统，任务脚本来源

##3、使用流程

① 调用initMetaInfoToMysql，初始化Hive MetaStore信息。

② 调用initHeraInfo，初始化调度任务的脚本信息，读取到内存中。

③ 调用initTableColumnInfo，初始化所有表与字段信息，读取到内存中。

④ 调用initGraph，开始执行血缘解析。

⑤ 调用getUselessTable，获取在血缘之外的无用表；调用searchTableRelation，查询Neo4j以获得血缘关系。