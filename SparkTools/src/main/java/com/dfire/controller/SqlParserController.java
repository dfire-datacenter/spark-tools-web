package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.dfire.entity.SparkPlanResultEntity;
import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.SQLResult;
import com.dfire.products.dao.Neo4jDaoImpl;
import com.dfire.products.parse.LineParser;
import com.dfire.products.util.DBUtil;
import com.dfire.products.util.MysqlMetaCache;
import com.dfire.products.util.Neo4jUtil;
import com.dfire.utils.HiveLineageUtils;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.*;

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

    private MysqlMetaCache mysqlMetaCache = new MysqlMetaCache();

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
    public List<SparkPlanResultEntity> hiveParser(@RequestBody String sqlAll) {
        List<SparkPlanResultEntity> list = new ArrayList<>();
        for (String sql : sqlAll.split("(?<!\\\\);")) {
            try {
                HiveLineageUtils hiveLineageUtils = new HiveLineageUtils();
                hiveLineageUtils.getLineageInfo(sql);
                list.add(new SparkPlanResultEntity(
                        true,
                        hiveLineageUtils.getInputTable(),
                        hiveLineageUtils.getOutputTable(),
                        hiveLineageUtils.getWithTable(),
                        ""));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }


    /**
     * 解析sql 并将得到的node存入mysql
     */
    @RequestMapping("/hive_column_parser")
    public void hiveColumnParser(@RequestBody String sql) throws Exception {
        long startParser = System.currentTimeMillis();
        Map<String, Integer> tableInfo = mysqlMetaCache.getTableInfo();
        Map<String, Integer> columnInfo = mysqlMetaCache.getColumnInfo();
        LineParser lineParser = new LineParser();
        List<SQLResult> list = lineParser.parse(sql);
        int relationId = 0;
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
                        String outputDbName = outputDbTableName.split("\\.")[0];
                        String outputTableName = outputDbTableName.split("\\.")[1];
                        int outputColumnId = columnInfo.get(outputDbTableName + "." + outputColumnName);
                        int outputTableId = tableInfo.get(outputDbTableName);
                        //from_column_name db.table.column 如BBB.bbb.activity_id
                        String[] fromArray = colLine.getColCondition().split("&");
                        boolean check = false;
                        for (String tmp : fromArray) {
                            String[] from = tmp.split("\\.");
                            if (from.length == 3) {
                                //处理Mysql逻辑
                                check = true;
                                String inputDbName = from[0];
                                String inputTableName = from[1];
                                String inputColumnName = from[2];
                                int inputColumnId = columnInfo.get(tmp);
                                int inputTableId = tableInfo.get(inputDbName + "." + inputTableName);
                                //condition 将'\''转化为~存，不然sql报错
                                String condition = colLine.getConditionSet().toString().replace('\'', '~');
                                //++relationId
                                //insert into data_lineage.data_lineage_relation
                                appendSql += "('" + (++relationId) + "','" + outputColumnId + "','"
                                        + outputTableId + "','" + inputTableId + "','"
                                        + inputColumnId + "','" + condition + "')";
                                if (relationId % 10 == 0) {
                                    dbUtil.doInsert(baseSql + appendSql);
                                    appendSql = "";
                                } else {
                                    appendSql += ",\n";
                                }

                                //处理Neo4j逻辑
                                Neo4jUtil neo4jUtil = new Neo4jUtil();
                                long startTable = System.currentTimeMillis();
                                //neo4j处理table之间血缘
                                if (!neo4jUtil.neo4jCheckTableNodeExist(inputDbName, inputTableName)) {
                                    if (!neo4jUtil.neo4jCreateTableNode(inputDbName, inputTableName)) {
                                        throw new Exception("Create neo4j table node error!");
                                    }
                                }
                                if (!neo4jUtil.neo4jCheckTableNodeExist(outputDbName, outputTableName)) {
                                    if (!neo4jUtil.neo4jCreateTableNode(outputDbName, outputTableName)) {
                                        throw new Exception("Create neo4j table node error!");
                                    }
                                }
                                if (!neo4jUtil.neo4jCheckTableRelationExist(inputDbName, inputTableName,
                                        outputDbName, outputTableName)) {
                                    if (!neo4jUtil.neo4jCreateTableRelation(inputDbName, inputTableName,
                                            outputDbName, outputTableName)) {
                                        throw new Exception("Create neo4j table relation error!");
                                    }
                                }
                                System.out.println("deal neo4j table :" + (System.currentTimeMillis() - startTable) + "ms");

                                //neo4j处理columns之间血缘
                                long startColumn = System.currentTimeMillis();
                                if (!neo4jUtil.neo4jCheckColumnNodeExist(inputDbName, inputTableName, inputColumnName)) {
                                    if (!neo4jUtil.neo4jCreateColumnNode(inputDbName, inputTableName, inputColumnName)) {
                                        throw new Exception("Create neo4j column node error!");
                                    }
                                }
                                if (!neo4jUtil.neo4jCheckColumnNodeExist(outputDbName, outputTableName, outputColumnName)) {
                                    if (!neo4jUtil.neo4jCreateColumnNode(outputDbName, outputTableName, outputColumnName)) {
                                        throw new Exception("Create neo4j column node error!");
                                    }
                                }
                                if (!neo4jUtil.neo4jCheckColumnRelationExist(inputDbName, inputTableName,
                                        inputColumnName, outputDbName,
                                        outputTableName, outputColumnName)) {
                                    if (!neo4jUtil.neo4jCreateColumnRelation(inputDbName, inputTableName,
                                            inputColumnName, outputDbName,
                                            outputTableName, outputColumnName)) {
                                        throw new Exception("Create neo4j column relation error!");
                                    }
                                }
                                System.out.println("deal neo4j column :" + (System.currentTimeMillis() - startColumn) + "ms");

                                //TODO neo4j处理jobs之间的血缘

                            }
                        }
                        if (!check) {
                            System.out.println("WARN!! No full input information parsed.");
                        }
                    }
                }
                //插入未满一批的部分
                if (!"".equals(appendSql)) {
                    dbUtil.doInsert(baseSql + appendSql.substring(0, appendSql.length() - 2));
                    appendSql = "";
                }
            }
            System.out.println("InputTables:" + sqlResult.getInputTables().toString());
            System.out.println("OutputTables:" + sqlResult.getOutputTables().toString());
            System.out.println("ColLineList:" + sqlResult.getColLineList().toString());
        }
        System.out.println("Parser time used:" + (System.currentTimeMillis() - startParser) + "ms");
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
