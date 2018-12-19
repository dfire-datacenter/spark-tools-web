package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.dfire.entity.SparkPlanResultEntity;
import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.SQLResult;
import com.dfire.products.dao.Neo4jDaoImpl;
import com.dfire.products.parse.LineParser;
import com.dfire.products.util.*;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SqlParserController
 *
 * @author ： HeGuoZi
 * @date ： 15:31 2018/11/13
 */
@RestController
@RequestMapping("/sql")
public class SqlParserController {

    // http://127.0.0.1:8089/sql/spark_parser

    private DBUtil dbUtil = new DBUtil();
    private List<HeraJobEntity> heraJobList;

    MagicSnowFlake msf = new MagicSnowFlake(1, 1);

    @RequestMapping("/spark_parser")
    public SparkPlanResultEntity parser(@RequestBody String sql) {
        SQLConf sqlConf = new SQLConf();
        CatalystSqlParser catalystSqlParser = new CatalystSqlParser(sqlConf);
        ResolveLogicalPlan resolveLogicalPlan = new ResolveLogicalPlan();
        String inputTables = "";
        String outputTables = "";
        try {
            LogicalPlan logicalPlan = catalystSqlParser.parsePlan(sql);
            Tuple2<Set<ResolveLogicalPlan.DcTable>, Set<ResolveLogicalPlan.DcTable>> rlp = resolveLogicalPlan.resolvePlan(logicalPlan, "default");
            Set<ResolveLogicalPlan.DcTable> inputTableSet = rlp._1;
            Set<ResolveLogicalPlan.DcTable> outputTableSet = rlp._2;
            for (Iterator it = inputTableSet.iterator(); it.hasNext(); ) {
                if (!"".equals(inputTables)) {
                    inputTables += ";";
                }
                inputTables += it.next().toString();
            }
            for (Iterator it = outputTableSet.iterator(); it.hasNext(); ) {
                if (!"".equals(outputTables)) {
                    outputTables += ";";
                }
                outputTables += it.next().toString();
            }
            System.out.println(inputTables);
            System.out.println(outputTables);
            System.out.println(logicalPlan.toString());
        } catch (Exception e) {
            return new SparkPlanResultEntity(
                    false,
                    null,
                    null,
                    null,
                    e.toString()
            );
        }
        return new SparkPlanResultEntity(
                true,
                inputTables,
                outputTables,
                null,
                ""
        );
    }

    @RequestMapping("/hive_parser")
    public List<SQLResult> hiveParser(@RequestBody String sql) {
        List<SQLResult> list = new ArrayList<>();
        LineParser lineParser = new LineParser();
        try {
            list = lineParser.parse(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 扫描hive 库表字段信息 存入mysql
     */
    @RequestMapping("/initMetaInfoToMysql")
    public String initMetaInfoToMysql() {
        long start = System.currentTimeMillis();
        HiveClient.initMetaInfoToMysql();
        return "Successfully init MetaInfo to Mysql!\n" + "Time cost:" + (System.currentTimeMillis() - start) + "ms";
    }

    /**
     * 扫描hera hive spark任务 存入mysql
     */
    @RequestMapping("/initHeraInfo")
    public String initHeraInfo() {
        long start = System.currentTimeMillis();
        heraJobList = HeraUtil.getHeraJobList();
        String message = "Successfully init Hera scripts!\n" + "Time cost:" + (System.currentTimeMillis() - start) + "ms";
        System.out.println(message);
        return message;
    }

    /**
     * 解析hera sql 并将得到的node&relation存入neo4j和mysql
     */
    @RequestMapping("/initGraph")
    public String initGraph(@RequestBody Integer parseNum) throws Exception {
        long startParser = System.currentTimeMillis();
        MysqlMetaCache mysqlMetaCache = new MysqlMetaCache();
        Set<Long> problemJobList = new LinkedHashSet<>();
        Map<String, Long> tableInfo = mysqlMetaCache.getTableInfo();
        Map<String, Long> columnInfo = mysqlMetaCache.getColumnInfo();
        Neo4jUtil neo4jUtil = new Neo4jUtil();
        LineParser lineParser = new LineParser();
        if (heraJobList == null) {
            System.out.println("Please initMetaInfoToMysql and initHeraInfo first!");
            return "Please initMetaInfoToMysql and initHeraInfo first!";
        } else {
            System.out.println("HeraJobToTalNum:" + heraJobList.size());
            AtomicInteger number = new AtomicInteger(0);
            for (HeraJobEntity heraJobEntity : heraJobList) {
                System.out.println("HeraJob No:" + number.incrementAndGet());
                for (String sql : heraJobEntity.getScript().split("(?<!\\\\);")) {
                    List<SQLResult> list = lineParser.parse(sql);
                    AtomicInteger relationId = new AtomicInteger(0);
                    String baseSql = "insert into data_lineage.data_lineage_relation " +
                            "(`relation_id`,`column_id`,`table_id`,`from_table_id`,`from_column_id`,`condition`) values ";
                    for (SQLResult sqlResult : list) {
                        String appendSql = "";
                        Set<String> inputTables = sqlResult.getInputTables();
                        Set<String> outputTables = sqlResult.getOutputTables();
                        List<ColLine> colLineList = sqlResult.getColLineList();
                        for (String inputTable : inputTables) {
                            for (String outputTable : outputTables) {
                                for (ColLine colLine : colLineList) {
                                    //缓存表和字段这两张表，Map<String,int> 然后通过字段名获取对应字段id
                                    //column_name 如activity_id
                                    String outputColumnName = colLine.getToNameParse();
                                    String outputDbTableName = colLine.getToTable();
                                    String[] to = outputDbTableName.split("\\.");
                                    if (to.length == 2) {
                                        String outputDbName = to[0];
                                        String outputTableName = to[1];
                                        long outputColumnId;
                                        long outputTableId;
                                        try {
                                            outputColumnId = columnInfo.get(outputDbTableName + "." + outputColumnName);
                                            outputTableId = tableInfo.get(outputDbTableName);
                                        } catch (Exception e) {
                                            System.out.println("Attention! No table or column found in mysql meta tables.\n" +
                                                    "output:" + outputDbTableName + "." + outputColumnName);
                                            problemJobList.add(heraJobEntity.getId());
                                            outputColumnId = msf.nextId();
                                            outputTableId = msf.nextId();
                                        }

                                        //from_column_name db.table.column 如BBB.bbb.activity_id
                                        for (String conditionEnum : colLine.getFromNameSet()) {
                                            String[] fromArray = conditionEnum.split("&");
                                            for (String tmp : fromArray) {
                                                String[] from = tmp.split("\\.");
                                                if (from.length == 3) {
                                                    //处理Mysql逻辑
                                                    String inputDbName = from[0];
                                                    String inputTableName = from[1];
                                                    String inputColumnName = from[2];
                                                    long inputColumnId;
                                                    long inputTableId;
                                                    try {
                                                        inputColumnId = columnInfo.get(tmp);
                                                        inputTableId = tableInfo.get(inputDbName + "." + inputTableName);
                                                    } catch (Exception e) {
                                                        System.out.println("Attention! No table or column found in mysql meta tables.\n" +
                                                                "input:" + tmp);
                                                        problemJobList.add(heraJobEntity.getId());
                                                        inputColumnId = msf.nextId();
                                                        inputTableId = msf.nextId();
                                                    }
                                                    //condition 将'\''转化为~存，不然sql报错
                                                    String condition = colLine.getConditionSet().toString().replace('\'', '~');
                                                    //++relationId
                                                    relationId.getAndIncrement();
                                                    //insert into data_lineage.data_lineage_relation
                                                    appendSql += "('" + msf.nextId() + "','" + outputColumnId + "','"
                                                            + outputTableId + "','" + inputTableId + "','"
                                                            + inputColumnId + "','" + condition + "')";
                                                    if (Integer.parseInt(relationId.toString()) % 10 == 0) {
                                                        dbUtil.doInsert(baseSql + appendSql);
                                                        appendSql = "";
                                                    } else {
                                                        appendSql += ",\n";
                                                    }

                                                    //处理Neo4j逻辑
//                                                    long startTable = System.currentTimeMillis();
                                                    //neo4j处理table之间血缘
                                                    if (!neo4jUtil.neo4jCheckTableNodeExistByCache(inputDbName, inputTableName)) {
                                                        if (!neo4jUtil.neo4jCreateTableNode(inputDbName, inputTableName)) {
                                                            throw new Exception("Create neo4j table node error!");
                                                        }
                                                    }
                                                    if (!neo4jUtil.neo4jCheckTableNodeExistByCache(outputDbName, outputTableName)) {
                                                        if (!neo4jUtil.neo4jCreateTableNode(outputDbName, outputTableName)) {
                                                            throw new Exception("Create neo4j table node error!");
                                                        }
                                                    }
                                                    if (!neo4jUtil.neo4jCheckTableRelationExistByCache(inputDbName, inputTableName,
                                                            outputDbName, outputTableName)) {
                                                        if (!neo4jUtil.neo4jCreateTableRelation(inputDbName, inputTableName,
                                                                outputDbName, outputTableName)) {
                                                            throw new Exception("Create neo4j table relation error!");
                                                        }
                                                    }
//                                            System.out.println("deal neo4j table :" + (System.currentTimeMillis() - startTable) + "ms");

                                                    //neo4j处理columns之间血缘
//                                                    long startColumn = System.currentTimeMillis();
                                                    if (!neo4jUtil.neo4jCheckColumnNodeExistByCache(inputDbName, inputTableName, inputColumnName)) {
                                                        if (!neo4jUtil.neo4jCreateColumnNode(inputDbName, inputTableName, inputColumnName)) {
                                                            throw new Exception("Create neo4j column node error!");
                                                        }
                                                    }
                                                    if (!neo4jUtil.neo4jCheckColumnNodeExistByCache(outputDbName, outputTableName, outputColumnName)) {
                                                        if (!neo4jUtil.neo4jCreateColumnNode(outputDbName, outputTableName, outputColumnName)) {
                                                            throw new Exception("Create neo4j column node error!");
                                                        }
                                                    }
                                                    if (!neo4jUtil.neo4jCheckColumnRelationExistByCache(inputDbName, inputTableName,
                                                            inputColumnName, outputDbName,
                                                            outputTableName, outputColumnName)) {
                                                        if (!neo4jUtil.neo4jCreateColumnRelation(inputDbName, inputTableName,
                                                                inputColumnName, outputDbName,
                                                                outputTableName, outputColumnName)) {
                                                            throw new Exception("Create neo4j column relation error!");
                                                        }
                                                    }
//                                            System.out.println("deal neo4j column :" + (System.currentTimeMillis() - startColumn) + "ms");

                                                    //TODO neo4j处理jobs之间的血缘

                                                }
                                            }
                                        }
                                    } else {
                                        System.out.println("Error outputDbTableName:" + outputDbTableName);
                                    }
                                }
                            }
                            //插入未满一批的部分
                            if (!"".equals(appendSql)) {
                                dbUtil.doInsert(baseSql + appendSql.substring(0, appendSql.length() - 2));
                                appendSql = "";
                            }
                        }
//                    System.out.println("InputTables:" + sqlResult.getInputTables().toString());
//                    System.out.println("OutputTables:" + sqlResult.getOutputTables().toString());
//                    System.out.println("ColLineList:" + sqlResult.getColLineList().toString());
                    }
                }
            }
        }
        dbUtil.close();
        neo4jUtil.close();
        System.out.println("ProblemJobList size:" + problemJobList.size());
        System.out.println("ProblemJobList:" + problemJobList);
        System.out.println("Parser time used:" + (System.currentTimeMillis() - startParser) + "ms");
        return "Graph and relations have been successfully inited!";
    }

    @RequestMapping("/neo4j")
    public void neo4j() {
        Neo4jDaoImpl neo4jDao = new Neo4jDaoImpl();

        neo4jDao.executeSql("MATCH (a:TableNode),(b:TableNode)\n" +
                "WHERE a.database_name = \"bbb\" \n" +
                "AND a.table_name = \"aaa\" \n" +
                "AND b.database_name = \"bbb\" \n" +
                "AND b.table_name = \"aaa\"\n" +
                "CREATE (a)-[r:Flow]->(b)");

        System.out.println("1");
    }


}
