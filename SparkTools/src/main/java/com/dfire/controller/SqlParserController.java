package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.dfire.entity.SearchNeo4jEntity;
import com.dfire.entity.SparkPlanResultEntity;
import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.SQLResult;
import com.dfire.products.dao.Neo4jDaoImpl;
import com.dfire.products.hera.RenderHierarchyProperties;
import com.dfire.products.parse.LineParser;
import com.dfire.products.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.Context;
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
    private List<HeraJobEntity>       heraJobList;
    private Map<String, List<String>> tableColumnInfo;
    private Neo4jUtil             neo4jUtil    = new Neo4jUtil();
    private HashMap<String, Long> uselessTable = new HashMap<>(20000);

    private Configuration conf = new Configuration();

    private MagicSnowFlake msf = new MagicSnowFlake(1, 1);

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

    /**
     * 查询neo4j table relation
     */
    @RequestMapping("/searchTableRelation")
    public List<String> searchTableRelation(@RequestBody SearchNeo4jEntity searchNeo4jEntity) {
        return neo4jUtil.neo4jSearchTable(searchNeo4jEntity.getInputDatabaseName(),
                searchNeo4jEntity.getInputTableName(),
                searchNeo4jEntity.getOutputDatabaseName(),
                searchNeo4jEntity.getOutputTableName(),
                searchNeo4jEntity.getTreeDepth(),
                searchNeo4jEntity.getLimit());
    }

    /**
     * 提供无用表集合
     */
    @RequestMapping("/getUselessTable")
    public Set<String> getUselessTable() {
        return uselessTable.keySet();
    }

    @RequestMapping("/hive_parser")
    public List<SQLResult> hiveParser(@RequestBody String sql) {
        List<SQLResult> list = new ArrayList<>();
        LineParser lineParser = new LineParser();
        try {
            conf.set("_hive.hdfs.session.path", "local");
            conf.set("_hive.tmp_table_space", "local");
            conf.set("_hive.local.session.path", "local");
            conf.set("_tmp_space.db", "local");
            Context context = new Context(conf);
            list = lineParser.parse(RenderHierarchyProperties.render(sql), context);
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
    @RequestMapping("/initTableColumnInfo")
    public String initTableColumnInfo() {
        long start = System.currentTimeMillis();
        tableColumnInfo = HiveClient.initAndGetTableColumnInfo();
        return "Time cost:" + (System.currentTimeMillis() - start) + "ms";
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
        conf.set("_hive.hdfs.session.path", "local");
        conf.set("_hive.tmp_table_space", "local");
        conf.set("_hive.local.session.path", "local");
        conf.set("_tmp_space.db", "local");
        Context context = new Context(conf);
        MysqlMetaCache mysqlMetaCache = new MysqlMetaCache();
        Set<Long> problemTableList = new LinkedHashSet<>();
        Set<Long> problemColumnList = new LinkedHashSet<>();
        HashMap<String, Long> tableInfo = mysqlMetaCache.getTableInfo();
        uselessTable.putAll(tableInfo);
        Map<String, Long> columnInfo = mysqlMetaCache.getColumnInfo();
        LineParser lineParser = new LineParser();
        if (heraJobList == null) {
            System.out.println("Please initMetaInfoToMysql and initHeraInfo first!");
            dbUtil.close();
            neo4jUtil.close();
            return "Please initMetaInfoToMysql and initHeraInfo first!";
        } else {
            if (tableColumnInfo == null) {
                System.out.println("Please initTableColumnInfo or initMetaInfoToMysql first!");
                dbUtil.close();
                neo4jUtil.close();
                return "Please initTableColumnInfo or initMetaInfoToMysql first!";
            } else {
                System.out.println("HeraJobToTalNum:" + heraJobList.size());
                AtomicInteger number = new AtomicInteger(0);
                for (HeraJobEntity heraJobEntity : heraJobList) {
                    System.out.println("HeraJob No:" + number.incrementAndGet() + ";RealJobID:" + heraJobEntity.getId());
                    //快速测试 跳过检测过的job
//                    if (number.get() < 21) {
//                        continue;
//                    }
//                    if (heraJobEntity.getId() <= 669) {
//                        continue;
//                    }
                    List<SQLResult> list = lineParser.parse(RenderHierarchyProperties.render(heraJobEntity.getScript()), context);
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
                                    String outputDbTableName = colLine.getToTable();
                                    String outputColumnName;
                                    if (tableColumnInfo.get(outputDbTableName) != null
                                            && colLineList.size() == tableColumnInfo.get(outputDbTableName).size()) {
                                        outputColumnName = tableColumnInfo.get(outputDbTableName).get(colLineList.indexOf(colLine));
                                    } else {
                                        outputColumnName = colLine.getToNameParse();
                                    }
                                    String[] to = outputDbTableName.split("\\.");
                                    if (to.length == 2) {
                                        String outputDbName = to[0];
                                        String outputTableName = to[1];
                                        long outputColumnId = 1L;
                                        long outputTableId = 1L;
                                        try {
                                            outputColumnId = columnInfo.get(outputDbTableName + "." + outputColumnName);
                                            outputTableId = tableInfo.get(outputDbTableName);
                                            uselessTable.remove(outputDbTableName);
                                        } catch (Exception e) {
                                            System.out.println("Attention! No table or column found in mysql meta tables.\n" +
                                                    "output:" + outputDbTableName + "." + outputColumnName);
                                            if (outputColumnId != 1L) {
                                                problemTableList.add(heraJobEntity.getId());
                                            } else {
                                                problemColumnList.add(heraJobEntity.getId());
                                            }
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
                                                    long inputColumnId = 1L;
                                                    long inputTableId = 1L;
                                                    try {
                                                        inputColumnId = columnInfo.get(tmp);
                                                        inputTableId = tableInfo.get(inputDbName + "." + inputTableName);
                                                        uselessTable.remove(inputDbName + "." + inputTableName);
                                                    } catch (Exception e) {
                                                        System.out.println("Attention! No table or column found in mysql meta tables.\n" +
                                                                "input:" + tmp);
                                                        if (inputColumnId != 1L) {
                                                            problemTableList.add(heraJobEntity.getId());
                                                        } else {
                                                            problemColumnList.add(heraJobEntity.getId());
                                                        }
                                                    }
                                                    //condition 将'\''转化为~存，不然sql报错
                                                    String condition = colLine.getConditionSet().toString().replace('\'', '~');
                                                    //++relationId
                                                    relationId.getAndIncrement();
                                                    //insert into data_lineage.data_lineage_relation
                                                    appendSql += "('" + msf.nextId() + "','"
                                                            + outputColumnId + "','"
                                                            + outputTableId + "','"
                                                            + inputTableId + "','"
                                                            + inputColumnId + "','"
                                                            + condition + "')";
                                                    if (Integer.parseInt(relationId.toString()) % 10 == 0) {
                                                        dbUtil.doInsert(baseSql + appendSql);
                                                        appendSql = "";
                                                    } else {
                                                        appendSql += ",\n";
                                                    }

                                                    //处理Neo4j逻辑
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

                                                    //neo4j处理columns之间血缘
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

                                                    //TODO neo4j处理jobs之间的血缘

                                                }
                                            }
                                        }
                                    } else {
                                        if (!outputDbTableName.contains("TOK_TMP_FILE")) {
                                            problemTableList.add(heraJobEntity.getId());
                                            System.out.println("Error outputDbTableName:" + outputDbTableName);
                                        }
                                    }
                                }
                            }
                            //插入未满一批的部分
                            if (!"".equals(appendSql)) {
                                dbUtil.doInsert(baseSql + appendSql.substring(0, appendSql.length() - 2));
                                appendSql = "";
                            }
                        }
                    }
                }
            }
        }
        dbUtil.close();
        neo4jUtil.close();
        uselessTable.forEach((name, id) -> System.out.println("Useless table name:" + name + ";id:" + id));
        System.out.println("");
        System.out.println("problemColumnList size:" + problemColumnList.size());
        System.out.println("problemColumnList:" + problemColumnList);
        System.out.println("ProblemTableList size:" + problemTableList.size());
        System.out.println("ProblemTableList:" + problemTableList);
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
