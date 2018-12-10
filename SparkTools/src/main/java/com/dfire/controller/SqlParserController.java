package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.dfire.entity.SparkPlanResultEntity;
import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.SQLResult;
import com.dfire.products.parse.LineParser;
import com.dfire.products.util.DBUtil;
import com.dfire.utils.HiveLineageUtils;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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


    @RequestMapping("/hive_column_parser")
    public void hiveColumnParser(@RequestBody String sql) {
        try {
            LineParser lineParser = new LineParser();
            List<SQLResult> list = lineParser.parse(sql);
            int relationId = 0;
            for (SQLResult sqlResult : list) {
                Set<String> inputTables = sqlResult.getInputTables();
                Set<String> outputTables = sqlResult.getOutputTables();
                List<ColLine> colLineList = sqlResult.getColLineList();

                for (String inputTable : inputTables) {
//                    String inputDbName = inputTable.split(".")[0];
//                    String inputTableName = inputTable.split(".")[1];
                    for (String outputTable : outputTables) {
//                        String outputDbName = outputTable.split(".")[0];
//                        String outputTableName = outputTable.split(".")[1];
                        for (ColLine colLine : colLineList) {
                            //缓存表和字段这两张表，Map<String,int> 然后通过字段名获取对应字段id
                            //column_name 如activity_id
                            colLine.getToNameParse();
                            //from_column_name db.table.column 如BBB.bbb.activity_id
                            String[] from = colLine.getColCondition().split(".");
                            String inputDbName = from[0];
                            String inputTableName = from[1];
                            String inputColumnName = from[2];
                            //condition 直接存
                            String condition = colLine.getConditionSet().toString();
                            //++relationId
                            //insert into data_lineage.data_lineage_relation
                        }
                    }
                }

//                dbUtil.doInsert("");

                System.out.println("InputTables:" + sqlResult.getInputTables().toString());
                System.out.println("OutputTables:" + sqlResult.getOutputTables().toString());
                System.out.println("ColLineList:" + sqlResult.getColLineList().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
