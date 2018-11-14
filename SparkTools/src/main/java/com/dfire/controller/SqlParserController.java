package com.dfire.controller;

import com.dfire.ResolveLogicalPlan;
import com.twodfire.share.result.Result;
import com.twodfire.share.result.ResultSupport;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.Iterator;
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

    // http://127.0.0.1:8089/spark/sql/parser

    @RequestMapping("/parser")
    public Result getToken(@RequestBody String sql) {
        Result result = new ResultSupport(false);
        SQLConf sqlConf = new SQLConf();
        CatalystSqlParser catalystSqlParser = new CatalystSqlParser(sqlConf);
        ResolveLogicalPlan resolveLogicalPlan = new ResolveLogicalPlan();
        try {
            LogicalPlan logicalPlan = catalystSqlParser.parsePlan(sql);
            Tuple2<Set<ResolveLogicalPlan.DcTable>, Set<ResolveLogicalPlan.DcTable>> rlp = resolveLogicalPlan.resolvePlan(logicalPlan, "default");
            Set<ResolveLogicalPlan.DcTable> inputTableSet = rlp._1;
            Set<ResolveLogicalPlan.DcTable> outputTableSet = rlp._2;
            String input = "";
            String output = "";
            for (Iterator it = inputTableSet.iterator(); it.hasNext(); ) {
                if (!"".equals(input)) {
                    input += ";";
                }
                input += it.next().toString();
            }
            for (Iterator it = outputTableSet.iterator(); it.hasNext(); ) {
                if (!"".equals(output)) {
                    output += ";";
                }
                output += it.next().toString();
            }
            System.out.println(input);
            System.out.println(output);
            System.out.println(logicalPlan.toString());
            result.setMessage(input + "&&" + output);
            result.setSuccess(true);
        } catch (Exception e) {
            result.setSuccess(false);
        }
        return result;
    }

}
