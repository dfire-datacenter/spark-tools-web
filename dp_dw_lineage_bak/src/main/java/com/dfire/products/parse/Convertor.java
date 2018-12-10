package com.dfire.products.parse;

import com.dfire.products.bean.ColLine;
import com.dfire.products.bean.ColumnNode;
import com.dfire.products.bean.RelationShip;
import com.dfire.products.bean.TableNode;
import com.dfire.products.util.Check;
import com.dfire.products.util.MetaCache;

import java.util.*;
import java.util.Map.Entry;

public class Convertor {

    public static List<TableNode> convertToTableNode() {
        List<TableNode> list = new ArrayList<>();
        for (Entry<String, List<ColumnNode>> entry : MetaCache.getInstance().getcMap().entrySet()) {
            List<ColumnNode> value = entry.getValue();
            TableNode tn = columnNodeToTableNode(value.get(0));
            list.add(tn);
        }
        return list;
    }


    public static Map<Long, List<ColumnNode>> convertToColumnNode() {
        Map<Long, List<ColumnNode>> map = new HashMap<>();
        for (Entry<String, List<ColumnNode>> entry : MetaCache.getInstance().getcMap().entrySet()) {
            List<ColumnNode> list = entry.getValue();
            map.put(list.get(0).getTableId(), list);
        }
        return map;
    }

    public static List<RelationShip> convertTableRS(Set<String> toSet, Set<String> fromSet) {
        List<RelationShip> rsList = new ArrayList<>();
        for (String ttable : toSet) {
            for (String ftable : fromSet) {
                RelationShip rs = new RelationShip();
                rs.setNode1Id(MetaCache.getInstance().getTableMap().get(ttable.toLowerCase()));
                rs.setNode2Id(MetaCache.getInstance().getTableMap().get(ftable.toLowerCase()));
                rsList.add(rs);
            }
        }
        return rsList;
    }


    public static List<RelationShip> convertColumnRS(List<ColLine> clList) {
        List<RelationShip> rsList = new ArrayList<>();
        for (ColLine cl : clList) {
            String toName = cl.getToName();
            Set<String> fromNameSet = cl.getFromNameSet();
            Set<String> allConditionSet = cl.getAllConditionSet();
            Map<String, List<String>> propertyMap = generatePropertyMap(allConditionSet);
            for (String fromName : fromNameSet) {
                RelationShip rs = new RelationShip();
                rs.setNode1Id(MetaCache.getInstance().getColumnMap().get(toName.toLowerCase()));
                rs.setNode2Id(MetaCache.getInstance().getColumnMap().get(fromName.toLowerCase()));
                rs.setPropertyMap(propertyMap);
                rsList.add(rs);
            }
        }
        return rsList;
    }


    private static Map<String, List<String>> generatePropertyMap(
            Set<String> allConditionSet) {
        Map<String, List<String>> propertyMap = new HashMap<>();
        for (String string : allConditionSet) {
            int indexOf = string.indexOf(":");
            if (indexOf > 0) {
                String key = string.substring(0, indexOf);
                List<String> list = propertyMap.get(key);
                if (Check.isEmpty(list)) {
                    list = new ArrayList<>();
                    propertyMap.put(key, list);
                }
                list.add(string.substring(indexOf + 1));
            }
        }
        return propertyMap;
    }


    private static TableNode columnNodeToTableNode(ColumnNode cn) {
        TableNode tn = new TableNode();
        tn.setId(cn.getTableId());
        tn.setDb(cn.getDb());
        tn.setTable(cn.getTable());
        return tn;
    }

    private Convertor() {

    }
}
