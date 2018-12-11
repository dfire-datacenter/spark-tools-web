package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.dfire.entity.SparkPlanResultEntity;
import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.SQLResult;
import com.dfire.products.dao.Neo4jDaoImpl;
import com.dfire.products.parse.LineParser;
import com.dfire.products.util.DBUtil;
import com.dfire.products.util.MysqlMetaCache;
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
 * @Description : SqlParserController
 * @Author ： HeGuoZi
 * @Date ： 15:31 2018/11/13
 * @Modified :
 */
@RestController
@RequestMapping("/sql")
public class SqlParserController {

    // http://127.0.0.1:8089/sql/spark_parser

    DBUtil dbUtil = new DBUtil();

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
     *
     * @param sql
     * @throws Exception
     */
    @RequestMapping("/hive_column_parser")
    public void hiveColumnParser(@RequestBody String sql) throws Exception {
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
                                
                            }                       }
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
    }

    @RequestMapping("/neo4j")
    public void neo4j() {
        Neo4jDaoImpl neo4jDao = new Neo4jDaoImpl();

        neo4jDao.initExecute("CREATE (s:nn{name:\"heguozi\"})-[r:have]->(d:dd{something:\"another shit\"}) return s,d,r");

        System.out.println("1");
    }


}
