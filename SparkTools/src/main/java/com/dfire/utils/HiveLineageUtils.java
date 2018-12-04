package com.dfire.utils;

import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.*;

/**
 * @Description :  HiveLineageUtils
 * @Author ： HeGuoZi
 * @Date ： 15:45 2018/12/3
 * @Modified :
 */
public class HiveLineageUtils implements NodeProcessor {

    TreeSet<String> inputTableList  = new TreeSet<>();
    TreeSet<String> outputTableList = new TreeSet<>();

    //存放with子句中的别名, 最终的输入表是 inputTableList减去withTableList
    TreeSet<String> withTableList = new TreeSet<>();

    public String getInputTable() {
        String inputTables = "";
        for (Iterator it = inputTableList.iterator(); it.hasNext(); ) {
            if (!"".equals(inputTables)) {
                inputTables += ";";
            }
            inputTables += it.next().toString();
        }
        return inputTables;
    }

    public String getOutputTable() {
        String outputTables = "";
        for (Iterator it = outputTableList.iterator(); it.hasNext(); ) {
            if (!"".equals(outputTables)) {
                outputTables += ";";
            }
            outputTables += it.next().toString();
        }
        return outputTables;
    }

    public String getWithTable() {
        String withTables = "";
        for (Iterator it = withTableList.iterator(); it.hasNext(); ) {
            if (!"".equals(withTables)) {
                withTables += ";";
            }
            withTables += it.next().toString();
        }
        return withTables;
    }

    @Override
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;
        switch (pt.getToken().getType()) {
            //create语句
            case HiveParser.TOK_CREATETABLE: {
                String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(createName);
                break;
            }

            //insert语句
            case HiveParser.TOK_TAB: {
                // System.out.println(pt.getChildCount() + "tab");
                String insertName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(insertName);
                //  System.out.println("insertName  " + insertName);
                break;
            }

            //from语句
            case HiveParser.TOK_TABREF: {
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String fromName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableList.add(fromName);
                break;
            }

            // with.....语句
            case HiveParser.TOK_CTE: {
                for (int i = 0; i < pt.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    withTableList.add(cteName);
                }
                break;
            }
        }
        return null;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {

        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        inputTableList.clear();
        outputTableList.clear();
        withTableList.clear();

        Map<Rule, NodeProcessor> rules = new LinkedHashMap<>();

        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }
}
